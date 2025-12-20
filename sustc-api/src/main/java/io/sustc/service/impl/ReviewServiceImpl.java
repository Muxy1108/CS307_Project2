package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.ReviewService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@Slf4j
public class ReviewServiceImpl implements ReviewService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private RecipeServiceImpl recipeService;

    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, double rating, String review) {
        // 验证参数
        if (auth == null || !StringUtils.hasText(auth.getPassword())) {
            throw new SecurityException("Invalid auth");
        }

        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }

        // 验证用户
        long userId = validateActiveUser(auth);

        // 验证食谱是否存在且未被删除
        Map<String, Object> recipe;
        try {
            recipe = jdbcTemplate.queryForMap(
                    "SELECT r.recipeid, u.isdeleted as author_deleted " +
                            "FROM recipes r " +
                            "JOIN users u ON r.authorid = u.authorid " +
                            "WHERE r.recipeid = ?",
                    recipeId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe not found");
        }

        Boolean authorDeleted = (Boolean) recipe.get("author_deleted");
        if (Boolean.TRUE.equals(authorDeleted)) {
            throw new IllegalArgumentException("Recipe author is deleted");
        }

        // 检查用户是否已经评论过这个食谱
        Integer existingReview = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reviews WHERE recipeid = ? AND authorid = ?",
                Integer.class,
                recipeId, userId
        );

        if (existingReview != null && existingReview > 0) {
            throw new SecurityException("User has already reviewed this recipe");
        }

        // 生成reviewId
        long reviewId;
        try {
            reviewId = jdbcTemplate.queryForObject(
                    "SELECT COALESCE(MAX(reviewid), 0) + 1 FROM reviews",
                    Long.class
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate review id", e);
        }

        // 插入评论
        Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(
                "INSERT INTO reviews (reviewid, recipeid, authorid, rating, review, datesubmitted, datemodified) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                reviewId, recipeId, userId, rating, review, now, now
        );

        // 刷新食谱评分
        refreshRecipeAggregatedRating(recipeId);

        return reviewId;
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        // 验证参数
        if (auth == null || !StringUtils.hasText(auth.getPassword())) {
            throw new SecurityException("Invalid auth");
        }

        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("Rating must be between 1 and 5");
        }

        // 验证用户
        long userId = validateActiveUser(auth);

        // 验证评论是否存在且属于指定食谱和用户
        Map<String, Object> existingReview;
        try {
            existingReview = jdbcTemplate.queryForMap(
                    "SELECT authorid, recipeid FROM reviews WHERE reviewid = ?",
                    reviewId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review not found");
        }

        Long reviewAuthorId = ((Number) existingReview.get("authorid")).longValue();
        Long reviewRecipeId = ((Number) existingReview.get("recipeid")).longValue();

        // 检查权限
        if (reviewAuthorId != userId) {
            throw new SecurityException("Not authorized to edit this review");
        }

        // 检查评论是否属于指定食谱
        if (reviewRecipeId != recipeId) {
            throw new IllegalArgumentException("Review does not belong to the specified recipe");
        }

        // 更新评论
        Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(
                "UPDATE reviews SET rating = ?, review = ?, datemodified = ? WHERE reviewid = ?",
                rating, review, now, reviewId
        );

        // 刷新食谱评分
        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        // 验证参数
        if (auth == null || !StringUtils.hasText(auth.getPassword())) {
            throw new SecurityException("Invalid auth");
        }

        // 验证用户
        long userId = validateActiveUser(auth);

        // 验证评论是否存在且属于指定食谱
        Map<String, Object> existingReview;
        try {
            existingReview = jdbcTemplate.queryForMap(
                    "SELECT authorid, recipeid FROM reviews WHERE reviewid = ?",
                    reviewId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review not found");
        }

        Long reviewAuthorId = ((Number) existingReview.get("authorid")).longValue();
        Long reviewRecipeId = ((Number) existingReview.get("recipeid")).longValue();

        // 检查权限（只能删除自己的评论）
        if (reviewAuthorId != userId) {
            throw new SecurityException("Not authorized to delete this review");
        }

        // 检查评论是否属于指定食谱
        if (reviewRecipeId != recipeId) {
            throw new IllegalArgumentException("Review does not belong to the specified recipe");
        }

        // 删除评论点赞
        jdbcTemplate.update("DELETE FROM review_likes WHERE reviewid = ?", reviewId);

        // 删除评论
        jdbcTemplate.update("DELETE FROM reviews WHERE reviewid = ?", reviewId);

        // 刷新食谱评分
        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        // 验证参数
        if (auth == null || !StringUtils.hasText(auth.getPassword())) {
            throw new SecurityException("Invalid auth");
        }

        // 验证用户
        long userId = validateActiveUser(auth);

        // 验证评论是否存在
        Map<String, Object> review;
        try {
            review = jdbcTemplate.queryForMap(
                    "SELECT authorid FROM reviews WHERE reviewid = ?",
                    reviewId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review not found");
        }

        Long reviewAuthorId = ((Number) review.get("authorid")).longValue();

        // 检查用户是否尝试点赞自己的评论
        if (reviewAuthorId == userId) {
            throw new SecurityException("Cannot like your own review");
        }

        // 检查是否已经点赞过
        Integer existingLike = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM review_likes WHERE reviewid = ? AND authorid = ?",
                Integer.class,
                reviewId, userId
        );

        if (existingLike == null || existingLike == 0) {
            // 添加点赞
            jdbcTemplate.update(
                    "INSERT INTO review_likes (reviewid, authorid) VALUES (?, ?)",
                    reviewId, userId
            );
        }

        // 返回当前点赞总数
        Long totalLikes = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM review_likes WHERE reviewid = ?",
                Long.class,
                reviewId
        );

        return totalLikes != null ? totalLikes : 0;
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        // 验证参数
        if (auth == null || !StringUtils.hasText(auth.getPassword())) {
            throw new SecurityException("Invalid auth");
        }

        // 验证用户
        long userId = validateActiveUser(auth);

        // 验证评论是否存在
        try {
            jdbcTemplate.queryForObject(
                    "SELECT 1 FROM reviews WHERE reviewid = ?",
                    Integer.class,
                    reviewId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Review not found");
        }

        // 删除点赞（如果存在）
        jdbcTemplate.update(
                "DELETE FROM review_likes WHERE reviewid = ? AND authorid = ?",
                reviewId, userId
        );

        // 返回当前点赞总数
        Long totalLikes = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM review_likes WHERE reviewid = ?",
                Long.class,
                reviewId
        );

        return totalLikes != null ? totalLikes : 0;
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        // 验证参数
        if (page < 1) {
            throw new IllegalArgumentException("Page must be at least 1");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be positive");
        }

        // 验证食谱是否存在
        try {
            jdbcTemplate.queryForObject(
                    "SELECT 1 FROM recipes WHERE recipeid = ?",
                    Integer.class,
                    recipeId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe not found");
        }

        // 构建排序子句
        String orderByClause;
        if ("likes_desc".equalsIgnoreCase(sort)) {
            orderByClause = "ORDER BY like_count DESC, r.datemodified DESC NULLS LAST, r.reviewid ASC";
        } else if ("date_desc".equalsIgnoreCase(sort)) {
            orderByClause = "ORDER BY r.datemodified DESC NULLS LAST, r.reviewid ASC";
        } else {
            orderByClause = "ORDER BY r.reviewid ASC";
        }

        // 计算总数
        String countSql = "SELECT COUNT(*) FROM reviews WHERE recipeid = ?";
        Long total = jdbcTemplate.queryForObject(countSql, Long.class, recipeId);
        if (total == null) total = 0L;

        // 计算分页
        int offset = (page - 1) * size;

        // 构建查询SQL
        String querySql;
        if ("likes_desc".equalsIgnoreCase(sort)) {
            querySql = "SELECT r.*, " +
                    "COALESCE(l.like_count, 0) as like_count " +
                    "FROM reviews r " +
                    "LEFT JOIN (SELECT reviewid, COUNT(*) as like_count FROM review_likes GROUP BY reviewid) l " +
                    "ON r.reviewid = l.reviewid " +
                    "WHERE r.recipeid = ? " +
                    orderByClause + " " +
                    "LIMIT ? OFFSET ?";
        } else {
            querySql = "SELECT r.* FROM reviews r " +
                    "WHERE r.recipeid = ? " +
                    orderByClause + " " +
                    "LIMIT ? OFFSET ?";
        }

        // 执行查询获取评论基本信息
        List<Map<String, Object>> reviewRows;
        if ("likes_desc".equalsIgnoreCase(sort)) {
            reviewRows = jdbcTemplate.queryForList(querySql, recipeId, size, offset);
        } else {
            reviewRows = jdbcTemplate.queryForList(querySql, recipeId, size, offset);
        }

        // 收集评论ID用于批量查询点赞用户
        List<Long> reviewIds = new ArrayList<>();
        for (Map<String, Object> row : reviewRows) {
            reviewIds.add(((Number) row.get("reviewid")).longValue());
        }

        // 批量查询点赞用户
        Map<Long, long[]> likesMap = new HashMap<>();
        if (!reviewIds.isEmpty()) {
            // 构建IN查询
            StringBuilder inClause = new StringBuilder();
            for (int i = 0; i < reviewIds.size(); i++) {
                if (i > 0) inClause.append(",");
                inClause.append("?");
            }

            String likesSql = "SELECT reviewid, authorid FROM review_likes " +
                    "WHERE reviewid IN (" + inClause + ") " +
                    "ORDER BY reviewid, authorid";

            List<Map<String, Object>> likeRows = jdbcTemplate.queryForList(
                    likesSql, reviewIds.toArray()
            );

            // 按评论ID分组点赞用户
            for (Map<String, Object> likeRow : likeRows) {
                Long rid = ((Number) likeRow.get("reviewid")).longValue();
                Long uid = ((Number) likeRow.get("authorid")).longValue();

                List<Long> userList = new ArrayList<>();
                if (likesMap.containsKey(rid)) {
                    long[] existing = likesMap.get(rid);
                    for (long id : existing) userList.add(id);
                }
                userList.add(uid);

                long[] userIds = new long[userList.size()];
                for (int i = 0; i < userList.size(); i++) {
                    userIds[i] = userList.get(i);
                }
                likesMap.put(rid, userIds);
            }
        }

        // 构建ReviewRecord列表
        List<ReviewRecord> reviews = new ArrayList<>();
        for (Map<String, Object> row : reviewRows) {
            Long reviewId = ((Number) row.get("reviewid")).longValue();
            Long authorId = ((Number) row.get("authorid")).longValue();

            // 获取作者名
            String authorName = jdbcTemplate.queryForObject(
                    "SELECT authorname FROM users WHERE authorid = ?",
                    String.class,
                    authorId
            );

            ReviewRecord reviewRecord = ReviewRecord.builder()
                    .reviewId(reviewId)
                    .recipeId(((Number) row.get("recipeid")).longValue())
                    .authorId(authorId)
                    .authorName(authorName)
                    .rating(row.get("rating") != null ? ((Number) row.get("rating")).intValue() : 0)
                    .review((String) row.get("review"))
                    .dateSubmitted((Timestamp) row.get("datesubmitted"))
                    .dateModified((Timestamp) row.get("datemodified"))
                    .likes(likesMap.getOrDefault(reviewId, new long[0]))
                    .build();

            reviews.add(reviewRecord);
        }

        return PageResult.<ReviewRecord>builder()
                .items(reviews)
                .page(page)
                .size(size)
                .total(total)
                .build();
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        // 验证食谱是否存在
        Map<String, Object> recipe;
        try {
            recipe = jdbcTemplate.queryForMap(
                    "SELECT r.*, u.authorname " +
                            "FROM recipes r " +
                            "JOIN users u ON r.authorid = u.authorid " +
                            "WHERE r.recipeid = ?",
                    recipeId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe not found");
        }

        // 计算当前有效评论的统计信息
        Map<String, Object> stats = jdbcTemplate.queryForMap(
                "SELECT " +
                        "COUNT(*) as review_count, " +
                        "ROUND(AVG(rating)::numeric, 2) as avg_rating " +
                        "FROM reviews " +
                        "WHERE recipeid = ?",
                recipeId
        );

        Integer reviewCount = ((Number) stats.get("review_count")).intValue();
        Float avgRating = stats.get("avg_rating") != null ?
                ((Number) stats.get("avg_rating")).floatValue() : null;

        // 更新食谱的评分和评论数
        if (reviewCount == 0) {
            jdbcTemplate.update(
                    "UPDATE recipes SET aggregatedrating = NULL, reviewcount = 0 WHERE recipeid = ?",
                    recipeId
            );
        } else {
            jdbcTemplate.update(
                    "UPDATE recipes SET aggregatedrating = ?, reviewcount = ? WHERE recipeid = ?",
                    avgRating, reviewCount, recipeId
            );
        }

        // 返回更新后的食谱记录
        return recipeService.getRecipeById(recipeId);
    }

    //验证用户是否活跃（未删除）并验证密码

    private long validateActiveUser(AuthInfo auth) {
        if (auth == null) {
            throw new SecurityException("Invalid auth");
        }
        try {
            Map<String, Object> user = jdbcTemplate.queryForMap(
                    "SELECT authorid, isdeleted, password FROM users WHERE authorid = ?",
                    auth.getAuthorId()
            );
            Boolean deleted = (Boolean) user.get("isdeleted");
            if (Boolean.TRUE.equals(deleted)) {
                throw new SecurityException("Inactive user");
            }

            // 验证密码
            String storedPassword = (String) user.get("password");
            if (!Objects.equals(storedPassword, auth.getPassword())) {
                throw new SecurityException("Invalid password");
            }

            return ((Number) user.get("authorid")).longValue();
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("User not found");
        }
    }
}