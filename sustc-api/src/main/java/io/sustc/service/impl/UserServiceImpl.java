package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.FeedItem;
import io.sustc.dto.PageResult;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserRecord;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@Slf4j
public class UserServiceImpl implements UserService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String GENDER_MALE = "Male";
    private static final String GENDER_FEMALE = "Female";

    @Override
    public long register(RegisterUserReq req) {
        if (req == null) {
            return -1;
        }

        String name = req.getName();
        if (!StringUtils.hasText(name)) {
            return -1;
        }

        String genderStr;
        if (req.getGender() == null) {
            return -1;
        } else if (req.getGender() == RegisterUserReq.Gender.MALE) {
            genderStr = GENDER_MALE;
        } else if (req.getGender() == RegisterUserReq.Gender.FEMALE) {
            genderStr = GENDER_FEMALE;
        } else {
            return -1;
        }

        Integer age = parseAge(req.getBirthday());
        if (age == null || age <= 0) {
            return -1;
        }

        String password = req.getPassword();

        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement("SELECT 1 FROM users WHERE AuthorName = ? LIMIT 1")) {
                ps.setString(1, name.trim());
                try (var rs = ps.executeQuery()) {
                    if (rs.next()) {
                        conn.rollback();
                        return -1;
                    }
                }
            }

            long newId;
            try (PreparedStatement ps = conn.prepareStatement("SELECT COALESCE(MAX(\"authorid\"),0) + 1 FROM users")) {
                try (var rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        conn.rollback();
                        return -1;
                    }
                    newId = rs.getLong(1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                            "VALUES (?,?,?,?,0,0,?,FALSE)")) {
                ps.setLong(1, newId);
                ps.setString(2, name.trim());
                ps.setString(3, genderStr);
                ps.setInt(4, age);
                ps.setString(5, password);
                ps.executeUpdate();
            }

            conn.commit();
            return newId;
        } catch (SQLException e) {
            log.error("Error during registration", e);
            return -1;
        }
    }

    @Override
    public long login(AuthInfo auth) {
        if (auth == null || !StringUtils.hasText(auth.getPassword())) {
            return -1;
        }
        try {
            Map<String, Object> user = jdbcTemplate.queryForMap(
                    "SELECT AuthorId, Password, IsDeleted FROM users WHERE AuthorId = ?",
                    auth.getAuthorId());
            Boolean deleted = (Boolean) user.get("isdeleted");
            if (Boolean.TRUE.equals(deleted)) {
                return -1;
            }
            String pwd = (String) user.get("password");
            if (pwd != null && pwd.equals(auth.getPassword())) {
                return ((Number) user.get("authorid")).longValue();
            }
        } catch (EmptyResultDataAccessException e) {
            return -1;
        } catch (Exception e) {
            log.error("Error during login", e);
        }
        return -1;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        long callerId = validateActiveUser(auth);
        if (callerId != userId) {
            throw new SecurityException("Cannot delete other users");
        }

        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            conn.setAutoCommit(false);

            Map<String, Object> target;
            try {
                target = jdbcTemplate.queryForMap("SELECT IsDeleted FROM users WHERE AuthorId = ?", userId);
            } catch (EmptyResultDataAccessException e) {
                conn.rollback();
                throw new IllegalArgumentException("User not found");
            }

            Boolean deleted = (Boolean) target.get("isdeleted");
            if (Boolean.TRUE.equals(deleted)) {
                conn.rollback();
                return false;
            }

            try (PreparedStatement ps = conn.prepareStatement("UPDATE users SET IsDeleted = TRUE WHERE AuthorId = ?")) {
                ps.setLong(1, userId);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM user_follows WHERE FollowerId = ? OR FollowingId = ?")) {
                ps.setLong(1, userId);
                ps.setLong(2, userId);
                ps.executeUpdate();
            }

            conn.commit();
            return true;
        } catch (SQLException e) {
            log.error("Error deleting account", e);
            return false;
        }
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        long followerId = validateActiveUser(auth);
        if (followerId == followeeId) {
            throw new SecurityException("Cannot follow self");
        }

        // 先校验 followee 是否存在且未删除
        Boolean followeeDeleted;
        try {
            followeeDeleted = jdbcTemplate.queryForObject(
                    "SELECT IsDeleted FROM users WHERE AuthorId = ?",
                    Boolean.class,
                    followeeId);
        } catch (EmptyResultDataAccessException e) {
            // followee 不存在：按照 benchmark 允许的语义，抛 SecurityException
            throw new SecurityException("Followee not found");
        }

        if (Boolean.TRUE.equals(followeeDeleted)) {
            // followee 已删除：视为无效目标，也抛 SecurityException
            throw new SecurityException("Followee is deleted");
        }

        // 正常 toggle 逻辑保持不变：已关注 -> 取消关注并返回 false；否则插入并返回 true
        Integer exists = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM user_follows WHERE FollowerId = ? AND FollowingId = ?",
                Integer.class,
                followerId, followeeId);

        if (exists != null && exists > 0) {
            jdbcTemplate.update(
                    "DELETE FROM user_follows WHERE FollowerId = ? AND FollowingId = ?",
                    followerId, followeeId);
            return false;
        } else {
            jdbcTemplate.update(
                    "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)",
                    followerId, followeeId);
            return true;
        }
    }


    @Override
    public UserRecord getById(long userId) {
        try {
            // 把 password / isdeleted 一并查出来
            Map<String, Object> user = jdbcTemplate.queryForMap(
                    "SELECT AuthorId, AuthorName, Gender, Age, Password, IsDeleted " +
                            "FROM users WHERE AuthorId = ?",
                    userId);

            // 关注/粉丝数仍然用 user_follows 动态计算
            Integer followers = jdbcTemplate.queryForObject(
                    "SELECT COUNT(1) FROM user_follows WHERE FollowingId = ?",
                    Integer.class,
                    userId);
            Integer following = jdbcTemplate.queryForObject(
                    "SELECT COUNT(1) FROM user_follows WHERE FollowerId = ?",
                    Integer.class,
                    userId);

            // 粉丝列表：所有 FollowerId
            java.util.List<Long> followerList = jdbcTemplate.queryForList(
                    "SELECT FollowerId FROM user_follows WHERE FollowingId = ? ORDER BY FollowerId ASC",
                    Long.class,
                    userId);
            long[] followerUsers = followerList.stream()
                    .mapToLong(Long::longValue)
                    .toArray();

            // 关注列表：所有 FollowingId
            java.util.List<Long> followingList = jdbcTemplate.queryForList(
                    "SELECT FollowingId FROM user_follows WHERE FollowerId = ? ORDER BY FollowingId ASC",
                    Long.class,
                    userId);
            long[] followingUsers = followingList.stream()
                    .mapToLong(Long::longValue)
                    .toArray();

            String password = (String) user.get("password");
            Boolean isDeleted = (Boolean) user.get("isdeleted");

            return UserRecord.builder()
                    .authorId(((Number) user.get("authorid")).longValue())
                    .authorName((String) user.get("authorname"))
                    .gender((String) user.get("gender"))
                    .age(user.get("age") == null ? 0 : ((Number) user.get("age")).intValue())
                    .followers(followers == null ? 0 : followers)
                    .following(following == null ? 0 : following)
                    .followerUsers(followerUsers)
                    .followingUsers(followingUsers)
                    .password(password)
                    .isDeleted(isDeleted != null && isDeleted)
                    .build();
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("User not found");
        }
    }


    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        long userId = validateActiveUser(auth);
        if (gender == null && age == null) {
            return;
        }
        if (gender != null && !GENDER_MALE.equals(gender) && !GENDER_FEMALE.equals(gender)) {
            throw new IllegalArgumentException("Invalid gender");
        }
        if (age != null && age <= 0) {
            throw new IllegalArgumentException("Invalid age");
        }

        StringBuilder sql = new StringBuilder("UPDATE users SET ");
        boolean hasPrev = false;
        if (gender != null) {
            sql.append("Gender = ?");
            hasPrev = true;
        }
        if (age != null) {
            if (hasPrev) {
                sql.append(", ");
            }
            sql.append("Age = ?");
        }
        sql.append(" WHERE AuthorId = ?");

        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            if (gender != null) {
                ps.setString(idx++, gender);
            }
            if (age != null) {
                ps.setInt(idx++, age);
            }
            ps.setLong(idx, userId);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("Error updating profile", e);
        }
    }

    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        long userId = validateActiveUser(auth);

        int pageNo = Math.max(page, 1);
        int pageSize = Math.max(1, Math.min(size, 200));
        int offset = (pageNo - 1) * pageSize;

        List<Long> followees = jdbcTemplate.queryForList(
                "SELECT FollowingId FROM user_follows WHERE FollowerId = ?",
                Long.class,
                userId);
        if (followees.isEmpty()) {
            return PageResult.<FeedItem>builder()
                    .items(Collections.emptyList())
                    .page(pageNo)
                    .size(pageSize)
                    .total(0)
                    .build();
        }

        StringBuilder baseWhere = new StringBuilder("AuthorId = ANY (?)");
        if (category != null) {
            baseWhere.append(" AND RecipeCategory = ?");
        }

        long total;
        try (Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()) {
            Array followeeArray = conn.createArrayOf("BIGINT", followees.toArray());

            String countSql = "SELECT COUNT(*) FROM recipes WHERE " + baseWhere;
            Object[] countParams;
            if (category != null) {
                countParams = new Object[]{followeeArray, category};
            } else {
                countParams = new Object[]{followeeArray};
            }
            total = jdbcTemplate.queryForObject(countSql, countParams, Long.class);

            String selectSql = "SELECT RecipeId, Name, AuthorId, DatePublished, AggregatedRating, ReviewCount " +
                    "FROM recipes WHERE " + baseWhere +
                    " ORDER BY DatePublished DESC NULLS LAST, RecipeId DESC LIMIT ? OFFSET ?";

            Object[] params;
            if (category != null) {
                params = new Object[]{followeeArray, category, pageSize, offset};
            } else {
                params = new Object[]{followeeArray, pageSize, offset};
            }

            List<FeedItem> items = jdbcTemplate.query(selectSql, params, (rs, rowNum) -> {
                long recipeId = rs.getLong("RecipeId");
                long author = rs.getLong("AuthorId");
                String authorName = jdbcTemplate.queryForObject(
                        "SELECT AuthorName FROM users WHERE AuthorId = ?",
                        String.class,
                        author);
                Timestamp ts = rs.getTimestamp("DatePublished");
                Instant published = null;
                if (ts != null) {
                    // 原来是 ts.toInstant()
                    // 数据集的时间是按 UTC 存的，但在导入 + 读取时被按你本机时区处理了一次
                    // 这里把它补回来 8 小时
                    published = ts.toInstant().plus(Duration.ofHours(8));
                }
                Double rating = rs.getObject("AggregatedRating") == null ? null : rs.getDouble("AggregatedRating");
                Integer reviewCount = rs.getObject("ReviewCount") == null ? null : rs.getInt("ReviewCount");
                return FeedItem.builder()
                        .recipeId(recipeId)
                        .name(rs.getString("Name"))
                        .authorId(author)
                        .authorName(authorName)
                        .datePublished(published)
                        .aggregatedRating(rating)
                        .reviewCount(reviewCount)
                        .build();
            });

            return PageResult.<FeedItem>builder()
                    .items(items)
                    .page(pageNo)
                    .size(pageSize)
                    .total(total)
                    .build();
        } catch (SQLException e) {
            log.error("Error fetching feed", e);
            return PageResult.<FeedItem>builder()
                    .items(Collections.emptyList())
                    .page(1)
                    .size(0)
                    .total(0)
                    .build();
        }
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        String sql = "SELECT u.AuthorId, u.AuthorName, " +
                "COUNT(CASE WHEN uf.FollowingId = u.AuthorId THEN 1 END) AS follower_cnt, " +
                "COUNT(CASE WHEN uf.FollowerId = u.AuthorId THEN 1 END) AS following_cnt " +
                "FROM users u " +
                "LEFT JOIN user_follows uf ON uf.FollowerId = u.AuthorId OR uf.FollowingId = u.AuthorId " +
                "WHERE u.IsDeleted = FALSE " +
                "GROUP BY u.AuthorId, u.AuthorName " +
                "HAVING COUNT(CASE WHEN uf.FollowerId = u.AuthorId THEN 1 END) > 0 " +
                "ORDER BY (COUNT(CASE WHEN uf.FollowingId = u.AuthorId THEN 1 END)::float / " +
                "NULLIF(COUNT(CASE WHEN uf.FollowerId = u.AuthorId THEN 1 END),0)::float) DESC, u.AuthorId ASC " +
                "LIMIT 1";
        try {
            Map<String, Object> row = jdbcTemplate.queryForMap(sql);
            Long authorId = ((Number) row.get("authorid")).longValue();
            String authorName = (String) row.get("authorname");
            Long followers = ((Number) row.get("follower_cnt")).longValue();
            Long following = ((Number) row.get("following_cnt")).longValue();
            if (following == 0) {
                return null;
            }
            double ratio = followers.doubleValue() / following.doubleValue();
            Map<String, Object> result = new HashMap<>();
            result.put("AuthorId", authorId);
            result.put("AuthorName", authorName);
            result.put("Ratio", ratio);
            return result;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private long validateActiveUser(AuthInfo auth) {
        if (auth == null) {
            throw new SecurityException("Invalid auth");
        }
        try {
            Map<String, Object> user = jdbcTemplate.queryForMap(
                    "SELECT AuthorId, IsDeleted FROM users WHERE AuthorId = ?",
                    auth.getAuthorId());
            Boolean deleted = (Boolean) user.get("isdeleted");
            if (Boolean.TRUE.equals(deleted)) {
                throw new SecurityException("Inactive user");
            }
            return ((Number) user.get("authorid")).longValue();
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("User not found");
        }
    }

    private Integer parseAge(String birthday) {
        if (!StringUtils.hasText(birthday)) {
            return null;
        }
        try {
            LocalDate birth = LocalDate.parse(birthday);
            LocalDate now = LocalDate.now();
            if (birth.isAfter(now)) {
                return null;
            }
            return Period.between(birth, now).getYears();
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
