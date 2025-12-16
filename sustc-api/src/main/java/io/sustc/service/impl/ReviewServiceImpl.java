package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j

public class ReviewServiceImpl implements ReviewService {

    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        return -1L; // 表示失败
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        // do nothing
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        // do nothing
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        return -1L; // 表示失败/没变化
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        return -1L;
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        int pageNo = Math.max(1, page);
        int pageSize = Math.max(1, Math.min(size, 200));

        return PageResult.<ReviewRecord>builder()
                .items(java.util.Collections.emptyList())
                .page(pageNo)
                .size(pageSize)
                .total(0)
                .build();
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        return null;
    }
}
