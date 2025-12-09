package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.FeedItem;
import io.sustc.dto.PageResult;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserRecord;
import io.sustc.service.UserService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserServiceImpl implements UserService {

    private static final String URL = "jdbc:postgresql://localhost:5432/sustc";
    private static final String USER = "sustc";
    private static final String PASSWORD = "sustec";

    private Connection getConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    @Override
    public long register(RegisterUserReq req) {
        if (req == null || req.getName() == null || req.getName().isBlank()) {
            return -1;
        }
        if (req.getGender() == null || req.getGender() == RegisterUserReq.Gender.UNKNOWN) {
            return -1;
        }
        int age = parseAge(req.getBirthday());
        if (age <= 0) {
            return -1;
        }
        if (req.getPassword() == null || req.getPassword().isBlank()) {
            return -1;
        }

        String checkSql = "SELECT 1 FROM users WHERE AuthorName = ?";
        String idSql = "SELECT COALESCE(MAX(AuthorId), 0) + 1 FROM users";
        String insertSql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) "
                + "VALUES (?, ?, ?, ?, 0, 0, ?, FALSE)";

        try (Connection conn = getConnection()) {
            try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                checkStmt.setString(1, req.getName());
                try (ResultSet rs = checkStmt.executeQuery()) {
                    if (rs.next()) {
                        return -1;
                    }
                }
            }

            long newId = 1;
            try (PreparedStatement idStmt = conn.prepareStatement(idSql);
                 ResultSet rs = idStmt.executeQuery()) {
                if (rs.next()) {
                    newId = rs.getLong(1);
                }
            }

            try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                insertStmt.setLong(1, newId);
                insertStmt.setString(2, req.getName());
                insertStmt.setString(3, normalizeGender(req.getGender()));
                insertStmt.setInt(4, age);
                insertStmt.setString(5, req.getPassword());
                insertStmt.executeUpdate();
            }
            return newId;
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public long login(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0 || auth.getPassword() == null || auth.getPassword().isBlank()) {
            return -1;
        }

        String sql = "SELECT Password, IsDeleted FROM users WHERE AuthorId = ?";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getAuthorId());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return -1;
                }
                boolean deleted = rs.getBoolean("IsDeleted");
                String password = rs.getString("Password");
                if (deleted || password == null) {
                    return -1;
                }
                if (password.equals(auth.getPassword())) {
                    return auth.getAuthorId();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        long operatorId = validateActiveUser(auth);
        if (operatorId <= 0) {
            throw new SecurityException("Invalid auth");
        }
        UserRecord target = getById(userId);
        if (target == null) {
            throw new IllegalArgumentException("User not found");
        }
        if (target.isDeleted()) {
            return false;
        }
        if (operatorId != userId) {
            throw new SecurityException("No permission");
        }

        String deleteSql = "UPDATE users SET IsDeleted = TRUE WHERE AuthorId = ?";
        String clearFollowSql = "DELETE FROM user_follows WHERE FollowerId = ? OR FollowingId = ?";
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement delStmt = conn.prepareStatement(deleteSql);
                 PreparedStatement clearStmt = conn.prepareStatement(clearFollowSql)) {
                delStmt.setLong(1, userId);
                delStmt.executeUpdate();

                clearStmt.setLong(1, userId);
                clearStmt.setLong(2, userId);
                clearStmt.executeUpdate();
                conn.commit();
                return true;
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        long followerId = validateActiveUser(auth);
        if (followerId <= 0) {
            throw new SecurityException("Invalid auth");
        }
        if (followerId == followeeId) {
            throw new SecurityException("Cannot follow self");
        }
        if (!userExists(followeeId)) {
            return false;
        }

        String checkSql = "SELECT 1 FROM user_follows WHERE FollowerId = ? AND FollowingId = ?";
        String insertSql = "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)";
        String deleteSql = "DELETE FROM user_follows WHERE FollowerId = ? AND FollowingId = ?";

        try (Connection conn = getConnection()) {
            boolean alreadyFollow = false;
            try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                checkStmt.setLong(1, followerId);
                checkStmt.setLong(2, followeeId);
                try (ResultSet rs = checkStmt.executeQuery()) {
                    if (rs.next()) {
                        alreadyFollow = true;
                    }
                }
            }

            if (alreadyFollow) {
                try (PreparedStatement deleteStmt = conn.prepareStatement(deleteSql)) {
                    deleteStmt.setLong(1, followerId);
                    deleteStmt.setLong(2, followeeId);
                    deleteStmt.executeUpdate();
                    return false;
                }
            } else {
                try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                    insertStmt.setLong(1, followerId);
                    insertStmt.setLong(2, followeeId);
                    insertStmt.executeUpdate();
                    return true;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public UserRecord getById(long userId) {
        String sql = "SELECT AuthorId, AuthorName, Gender, Age, Password, IsDeleted FROM users WHERE AuthorId = ?";
        String followerSql = "SELECT COUNT(*) FROM user_follows WHERE FollowingId = ?";
        String followingSql = "SELECT COUNT(*) FROM user_follows WHERE FollowerId = ?";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             PreparedStatement followerStmt = conn.prepareStatement(followerSql);
             PreparedStatement followingStmt = conn.prepareStatement(followingSql)) {
            stmt.setLong(1, userId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalArgumentException("User not found");
                }
                followerStmt.setLong(1, userId);
                followingStmt.setLong(1, userId);
                int followers = 0;
                int following = 0;
                try (ResultSet fr = followerStmt.executeQuery()) {
                    if (fr.next()) {
                        followers = fr.getInt(1);
                    }
                }
                try (ResultSet fg = followingStmt.executeQuery()) {
                    if (fg.next()) {
                        following = fg.getInt(1);
                    }
                }
                UserRecord record = new UserRecord();
                record.setAuthorId(rs.getLong("AuthorId"));
                record.setAuthorName(rs.getString("AuthorName"));
                record.setGender(rs.getString("Gender"));
                record.setAge(rs.getInt("Age"));
                record.setFollowers(followers);
                record.setFollowing(following);
                record.setPassword(rs.getString("Password"));
                record.setDeleted(rs.getBoolean("IsDeleted"));
                return record;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        long userId = validateActiveUser(auth);
        if (userId <= 0) {
            throw new SecurityException("Invalid auth");
        }

        List<Object> params = new ArrayList<>();
        StringBuilder sb = new StringBuilder("UPDATE users SET ");
        boolean first = true;
        if (gender != null) {
            if (!isValidGender(gender)) {
                throw new IllegalArgumentException("Invalid gender");
            }
            sb.append("Gender = ?");
            params.add(gender);
            first = false;
        }
        if (age != null) {
            if (age <= 0) {
                throw new IllegalArgumentException("Invalid age");
            }
            if (!first) {
                sb.append(", ");
            }
            sb.append("Age = ?");
            params.add(age);
            first = false;
        }
        if (first) {
            return;
        }
        sb.append(" WHERE AuthorId = ?");
        params.add(userId);

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sb.toString())) {
            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        long userId = validateActiveUser(auth);
        if (userId <= 0) {
            throw new SecurityException("Invalid auth");
        }
        if (page < 1) {
            page = 1;
        }
        if (size < 1) {
            size = 1;
        } else if (size > 200) {
            size = 200;
        }

        String baseSql = "FROM recipes r JOIN users u ON r.AuthorId = u.AuthorId "
                + "WHERE r.AuthorId IN (SELECT FollowingId FROM user_follows WHERE FollowerId = ?)";
        if (category != null) {
            baseSql += " AND r.RecipeCategory = ?";
        }
        String countSql = "SELECT COUNT(*) " + baseSql;
        String querySql = "SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName, r.DatePublished, r.AggregatedRating, r.ReviewCount "
                + baseSql + " ORDER BY r.DatePublished DESC NULLS LAST, r.RecipeId DESC LIMIT ? OFFSET ?";

        List<FeedItem> items = new ArrayList<>();
        long total = 0;
        try (Connection conn = getConnection()) {
            try (PreparedStatement countStmt = conn.prepareStatement(countSql)) {
                countStmt.setLong(1, userId);
                if (category != null) {
                    countStmt.setString(2, category);
                }
                try (ResultSet rs = countStmt.executeQuery()) {
                    if (rs.next()) {
                        total = rs.getLong(1);
                    }
                }
            }

            try (PreparedStatement queryStmt = conn.prepareStatement(querySql)) {
                queryStmt.setLong(1, userId);
                int index = 2;
                if (category != null) {
                    queryStmt.setString(index++, category);
                }
                queryStmt.setInt(index++, size);
                queryStmt.setInt(index, (page - 1) * size);
                try (ResultSet rs = queryStmt.executeQuery()) {
                    while (rs.next()) {
                        FeedItem item = new FeedItem();
                        item.setRecipeId(rs.getLong("RecipeId"));
                        item.setName(rs.getString("Name"));
                        item.setAuthorId(rs.getLong("AuthorId"));
                        item.setAuthorName(rs.getString("AuthorName"));
                        Timestamp ts = rs.getTimestamp("DatePublished");
                        item.setDatePublished(ts == null ? null : Instant.ofEpochMilli(ts.getTime()));
                        item.setAggregatedRating(rs.getObject("AggregatedRating") == null ? null : rs.getDouble("AggregatedRating"));
                        item.setReviewCount(rs.getObject("ReviewCount") == null ? null : rs.getInt("ReviewCount"));
                        items.add(item);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        PageResult<FeedItem> result = new PageResult<>();
        result.setItems(items);
        result.setPage(page);
        result.setSize(size);
        result.setTotal(total);
        return result;
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        String sql = "SELECT u.AuthorId, u.AuthorName, "
                + "(SELECT COUNT(*) FROM user_follows f1 WHERE f1.FollowingId = u.AuthorId) AS follower_count, "
                + "(SELECT COUNT(*) FROM user_follows f2 WHERE f2.FollowerId = u.AuthorId) AS following_count "
                + "FROM users u WHERE u.IsDeleted = FALSE";
        long bestId = -1;
        String bestName = null;
        double bestRatio = -1.0;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                long id = rs.getLong("AuthorId");
                String name = rs.getString("AuthorName");
                int followers = rs.getInt("follower_count");
                int following = rs.getInt("following_count");
                if (following == 0) {
                    continue;
                }
                double ratio = followers * 1.0 / following;
                if (ratio > bestRatio || (Math.abs(ratio - bestRatio) < 1e-9 && id < bestId)) {
                    bestRatio = ratio;
                    bestId = id;
                    bestName = name;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (bestId == -1) {
            return null;
        }
        Map<String, Object> result = new HashMap<>();
        result.put("AuthorId", bestId);
        result.put("AuthorName", bestName);
        result.put("Ratio", bestRatio);
        return result;
    }

    private long validateActiveUser(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0) {
            return -1;
        }
        String sql = "SELECT IsDeleted FROM users WHERE AuthorId = ?";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getAuthorId());
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next() && !rs.getBoolean("IsDeleted")) {
                    return auth.getAuthorId();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    private boolean userExists(long userId) {
        String sql = "SELECT IsDeleted FROM users WHERE AuthorId = ?";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, userId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return !rs.getBoolean("IsDeleted");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private int parseAge(String birthday) {
        if (birthday == null || birthday.isBlank()) {
            return -1;
        }
        try {
            LocalDate date = LocalDate.parse(birthday);
            Period period = Period.between(date, LocalDate.now());
            return period.getYears();
        } catch (DateTimeParseException e) {
            return -1;
        }
    }

    private boolean isValidGender(String gender) {
        return "Male".equalsIgnoreCase(gender) || "Female".equalsIgnoreCase(gender);
    }

    private String normalizeGender(RegisterUserReq.Gender gender) {
        if (gender == null) {
            return null;
        }
        switch (gender) {
            case MALE:
                return "Male";
            case FEMALE:
                return "Female";
            default:
                return null;
        }
    }
}
