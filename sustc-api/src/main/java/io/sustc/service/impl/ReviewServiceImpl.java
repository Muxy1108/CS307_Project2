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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ReviewServiceImpl implements ReviewService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private RecipeServiceImpl recipeService;

    private static final int MIN_RATING = 1;
    private static final int MAX_RATING = 5;

    /**
     * 验证用户认证信息
     */
    private Map<String, Object> authenticateUser(AuthInfo auth) {
        if (auth == null || StringUtils.isEmpty(auth.getPassword())) {
            throw new SecurityException("无效的认证信息");
        }

        String sql = "SELECT authorid, isdeleted, password FROM users WHERE authorid = ?";
        try {
            Map<String, Object> user = jdbcTemplate.queryForMap(sql, auth.getAuthorId());

            if ((Boolean) user.get("isdeleted")) {
                throw new SecurityException("用户账户不可用");
            }

            if (!user.get("password").equals(auth.getPassword())) {
                throw new SecurityException("身份验证失败");
            }

            return user;
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("用户记录不存在");
        }
    }

    /**
     * 检查食谱是否有效
     */
    private void validateRecipe(long recipeId) {
        String checkSql = "SELECT 1 FROM recipes WHERE recipeid = ? AND EXISTS (" +
                "SELECT 1 FROM users u WHERE u.authorid = recipes.authorid AND NOT u.isdeleted)";
        try {
            jdbcTemplate.queryForObject(checkSql, Integer.class, recipeId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("无效的食谱ID或作者不可用");
        }
    }

    /**
     * 检查评论是否存在
     */
    private Map<String, Object> getReviewDetails(long reviewId) {
        String sql = "SELECT authorid, recipeid FROM reviews WHERE reviewid = ?";
        try {
            return jdbcTemplate.queryForMap(sql, reviewId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("未找到评论记录");
        }
    }

    /**
     * 检查用户是否已评论过该食谱
     */
    private boolean hasUserReviewed(long userId, long recipeId) {
        String sql = "SELECT EXISTS(SELECT 1 FROM reviews WHERE authorid = ? AND recipeid = ?)";
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(sql, Boolean.class, userId, recipeId));
    }

    /**
     * 生成新的评论ID
     */
    private long generateNewReviewId() {
        String sql = "SELECT COALESCE(MAX(reviewid), 0) + 1 FROM reviews";
        return jdbcTemplate.queryForObject(sql, Long.class);
    }

    //long addReview(AuthInfo auth, long recipeId, double rating, String review);
    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, double rating, String review) {
        // 参数验证
        if (rating < MIN_RATING || rating > MAX_RATING) {
            throw new IllegalArgumentException("评分值超出有效范围");
        }

        // 用户认证
        Map<String, Object> user = authenticateUser(auth);
        long userId = ((Number) user.get("authorid")).longValue();

        // 食谱验证
        validateRecipe(recipeId);

        // 检查重复评论
        if (hasUserReviewed(userId, recipeId)) {
            throw new SecurityException("每个用户只能对同一食谱评价一次");
        }

        // 生成新ID并插入
        long newId = generateNewReviewId();
        Timestamp now = new Timestamp(System.currentTimeMillis());

        String insertSql = "INSERT INTO reviews (reviewid, recipeid, authorid, rating, review, datesubmitted, datemodified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.update(insertSql, newId, recipeId, userId, rating, review, now, now);

        // 更新聚合评分
        updateRecipeRating(recipeId);

        return newId;
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        // 参数验证
        if (rating < MIN_RATING || rating > MAX_RATING) {
            throw new IllegalArgumentException("评分值无效");
        }

        // 用户认证
        Map<String, Object> user = authenticateUser(auth);
        long userId = ((Number) user.get("authorid")).longValue();

        // 获取评论信息
        Map<String, Object> reviewData = getReviewDetails(reviewId);
        long reviewAuthorId = ((Number) reviewData.get("authorid")).longValue();
        long reviewRecipeId = ((Number) reviewData.get("recipeid")).longValue();

        // 权限验证
        if (reviewAuthorId != userId) {
            throw new SecurityException("无权限修改他人评论");
        }

        if (reviewRecipeId != recipeId) {
            throw new IllegalArgumentException("评论与食谱不匹配");
        }

        // 更新评论
        Timestamp updateTime = new Timestamp(System.currentTimeMillis());
        String updateSql = "UPDATE reviews SET rating = ?, review = ?, datemodified = ? WHERE reviewid = ?";
        jdbcTemplate.update(updateSql, rating, review, updateTime, reviewId);

        // 更新聚合评分
        updateRecipeRating(recipeId);
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        // 用户认证
        Map<String, Object> user = authenticateUser(auth);
        long userId = ((Number) user.get("authorid")).longValue();

        // 获取评论信息
        Map<String, Object> reviewData = getReviewDetails(reviewId);
        long reviewAuthorId = ((Number) reviewData.get("authorid")).longValue();
        long reviewRecipeId = ((Number) reviewData.get("recipeid")).longValue();

        // 权限验证
        if (reviewAuthorId != userId) {
            throw new SecurityException("只能删除自己的评论");
        }

        if (reviewRecipeId != recipeId) {
            throw new IllegalArgumentException("评论与指定食谱无关");
        }

        // 删除相关点赞
        jdbcTemplate.update("DELETE FROM review_likes WHERE reviewid = ?", reviewId);

        // 删除评论
        jdbcTemplate.update("DELETE FROM reviews WHERE reviewid = ?", reviewId);

        // 更新聚合评分
        updateRecipeRating(recipeId);
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        // 用户认证
        Map<String, Object> user = authenticateUser(auth);
        long userId = ((Number) user.get("authorid")).longValue();

        // 检查评论存在性
        Map<String, Object> reviewData = getReviewDetails(reviewId);
        long reviewAuthorId = ((Number) reviewData.get("authorid")).longValue();

        // 防止自我点赞
        if (reviewAuthorId == userId) {
            throw new SecurityException("不能为自己的评论点赞");
        }

        // 检查是否已点赞
        String checkLikeSql = "SELECT COUNT(*) FROM review_likes WHERE reviewid = ? AND authorid = ?";
        int existingLikes = jdbcTemplate.queryForObject(checkLikeSql, Integer.class, reviewId, userId);

        // 添加点赞（如果未点赞）
        if (existingLikes == 0) {
            jdbcTemplate.update("INSERT INTO review_likes (reviewid, authorid) VALUES (?, ?)", reviewId, userId);
        }

        // 返回点赞总数
        String countSql = "SELECT COUNT(*) FROM review_likes WHERE reviewid = ?";
        return jdbcTemplate.queryForObject(countSql, Long.class, reviewId);
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        // 用户认证
        Map<String, Object> user = authenticateUser(auth);
        long userId = ((Number) user.get("authorid")).longValue();

        // 验证评论存在
        try {
            jdbcTemplate.queryForObject("SELECT 1 FROM reviews WHERE reviewid = ?", Integer.class, reviewId);
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("未找到指定评论");
        }

        // 移除点赞
        jdbcTemplate.update("DELETE FROM review_likes WHERE reviewid = ? AND authorid = ?", reviewId, userId);

        // 返回剩余点赞数
        return jdbcTemplate.queryForObject(
                "SELECT COALESCE(COUNT(*), 0) FROM review_likes WHERE reviewid = ?",
                Long.class, reviewId
        );
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        // 参数验证
        if (page < 1 || size <= 0) {
            throw new IllegalArgumentException("分页参数无效");
        }

        // 验证食谱存在
        validateRecipe(recipeId);

        // 计算总数和分页
        long total = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reviews WHERE recipeid = ?",
                Long.class, recipeId
        );

        int offset = (page - 1) * size;

        // 构建排序条件
        String orderClause = buildOrderClause(sort);

        // 查询评论数据
        String querySql = buildReviewQuerySql(sort);
        List<Map<String, Object>> reviewData = jdbcTemplate.queryForList(
                querySql, recipeId, size, offset
        );

        // 处理结果
        List<ReviewRecord> records = processReviewResults(reviewData);

        return PageResult.<ReviewRecord>builder()
                .items(records)
                .page(page)
                .size(size)
                .total(total)
                .build();
    }

    /**
     * 构建排序子句
     */
    private String buildOrderClause(String sort) {
        if ("likes_desc".equals(sort)) {
            return "ORDER BY like_count DESC, r.datemodified DESC, r.reviewid ASC";
        } else if ("date_desc".equals(sort)) {
            return "ORDER BY r.datemodified DESC, r.reviewid ASC";
        }
        return "ORDER BY r.reviewid ASC";
    }

    /**
     * 构建查询SQL
     */
    private String buildReviewQuerySql(String sort) {
        if ("likes_desc".equals(sort)) {
            return "SELECT r.*, COALESCE(l.like_count, 0) as like_count " +
                    "FROM reviews r " +
                    "LEFT JOIN (SELECT reviewid, COUNT(*) as like_count FROM review_likes GROUP BY reviewid) l " +
                    "ON r.reviewid = l.reviewid " +
                    "WHERE r.recipeid = ? " +
                    buildOrderClause(sort) + " LIMIT ? OFFSET ?";
        }
        return "SELECT r.* FROM reviews r WHERE r.recipeid = ? " +
                buildOrderClause(sort) + " LIMIT ? OFFSET ?";
    }

    /**
     * 处理评论结果
     */
    private List<ReviewRecord> processReviewResults(List<Map<String, Object>> reviewData) {
        if (reviewData.isEmpty()) {
            return Collections.emptyList();
        }

        // 收集评论ID
        List<Long> reviewIds = reviewData.stream()
                .map(row -> ((Number) row.get("reviewid")).longValue())
                .collect(Collectors.toList());

        // 批量获取点赞信息
        Map<Long, long[]> likesMap = getLikesForReviews(reviewIds);

        // 构建返回记录
        return reviewData.stream()
                .map(row -> createReviewRecord(row, likesMap))
                .collect(Collectors.toList());
    }

    /**
     * 批量获取点赞信息
     */
    private Map<Long, long[]> getLikesForReviews(List<Long> reviewIds) {
        if (reviewIds.isEmpty()) {
            return new HashMap<>();
        }

        String placeholders = String.join(",", Collections.nCopies(reviewIds.size(), "?"));
        String sql = "SELECT reviewid, authorid FROM review_likes WHERE reviewid IN (" + placeholders + ")";

        List<Map<String, Object>> likes = jdbcTemplate.queryForList(sql, reviewIds.toArray());

        // 按评论ID分组点赞用户
        Map<Long, List<Long>> groupedLikes = new HashMap<>();
        for (Map<String, Object> like : likes) {
            Long reviewId = ((Number) like.get("reviewid")).longValue();
            Long userId = ((Number) like.get("authorid")).longValue();

            groupedLikes.computeIfAbsent(reviewId, k -> new ArrayList<>()).add(userId);
        }

        // 转换为数组
        Map<Long, long[]> result = new HashMap<>();
        for (Map.Entry<Long, List<Long>> entry : groupedLikes.entrySet()) {
            long[] userIds = entry.getValue().stream().mapToLong(Long::longValue).toArray();
            result.put(entry.getKey(), userIds);
        }

        return result;
    }

    /**
     * 创建评论记录对象
     */
    private ReviewRecord createReviewRecord(Map<String, Object> row, Map<Long, long[]> likesMap) {
        Long reviewId = ((Number) row.get("reviewid")).longValue();
        Long authorId = ((Number) row.get("authorid")).longValue();

        // 获取作者名
        String authorName = jdbcTemplate.queryForObject(
                "SELECT authorname FROM users WHERE authorid = ?",
                String.class, authorId
        );

        return ReviewRecord.builder()
                .reviewId(reviewId)
                .recipeId(((Number) row.get("recipeid")).longValue())
                .authorId(authorId)
                .authorName(authorName != null ? authorName : "")
                .rating(((Number) row.get("rating")).floatValue())
                .review((String) row.get("review"))
                .dateSubmitted((Timestamp) row.get("datesubmitted"))
                .dateModified((Timestamp) row.get("datemodified"))
                .likes(likesMap.getOrDefault(reviewId, new long[0]))
                .build();
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        // 验证食谱存在
        validateRecipe(recipeId);

        // 计算统计信息
        Map<String, Object> stats = jdbcTemplate.queryForMap(
                "SELECT COUNT(*) as cnt, AVG(rating) as avg_rating FROM reviews WHERE recipeid = ?",
                recipeId
        );

        long count = ((Number) stats.get("cnt")).longValue();
        Object avgRatingObj = stats.get("avg_rating");
        Double avgRating = avgRatingObj != null ? ((Number) avgRatingObj).doubleValue() : null;

        // 更新食谱
        if (count == 0) {
            jdbcTemplate.update(
                    "UPDATE recipes SET aggregatedrating = NULL, reviewcount = 0 WHERE recipeid = ?",
                    recipeId
            );
        } else {
            jdbcTemplate.update(
                    "UPDATE recipes SET aggregatedrating = ROUND(CAST(? AS numeric), 2), reviewcount = ? WHERE recipeid = ?",
                    avgRating, count, recipeId
            );
        }

        // 返回更新后的食谱
        return recipeService.getRecipeById(recipeId);
    }

    /**
     * 更新食谱评分（内部方法）
     */
    private void updateRecipeRating(long recipeId) {
        refreshRecipeAggregatedRating(recipeId);
    }
}