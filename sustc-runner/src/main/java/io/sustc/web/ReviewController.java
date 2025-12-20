package io.sustc.web;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.ReviewService;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@Profile("server")
@RequestMapping("/api")
public class ReviewController {

    private final ReviewService reviewService;

    public ReviewController(ReviewService reviewService) {
        this.reviewService = reviewService;
    }

    public static class AddReviewReq {
        public AuthInfo auth;
        public double rating;
        public String review;
    }

    @PostMapping("/recipes/{recipeId}/reviews")
    public Map<String, Object> add(@PathVariable long recipeId, @RequestBody AddReviewReq req) {
        long id = reviewService.addReview(req.auth, recipeId, req.rating, req.review);
        return Map.of("reviewId", id);
    }

    public static class EditReviewReq {
        public AuthInfo auth;
        public int rating;
        public String review;
    }

    @PutMapping("/recipes/{recipeId}/reviews/{reviewId}")
    public Map<String, Object> edit(@PathVariable long recipeId, @PathVariable long reviewId,
                                    @RequestBody EditReviewReq req) {
        reviewService.editReview(req.auth, recipeId, reviewId, req.rating, req.review);
        return Map.of("ok", true);
    }

    public static class DeleteReviewReq {
        public AuthInfo auth;
    }

    @DeleteMapping("/recipes/{recipeId}/reviews/{reviewId}")
    public Map<String, Object> delete(@PathVariable long recipeId, @PathVariable long reviewId,
                                      @RequestBody DeleteReviewReq req) {
        reviewService.deleteReview(req.auth, recipeId, reviewId);
        return Map.of("ok", true);
    }

    public static class LikeReq {
        public AuthInfo auth;
    }

    @PostMapping("/reviews/{reviewId}/like")
    public Map<String, Object> like(@PathVariable long reviewId, @RequestBody LikeReq req) {
        long likes = reviewService.likeReview(req.auth, reviewId);
        return Map.of("likes", likes);
    }

    @DeleteMapping("/reviews/{reviewId}/like")
    public Map<String, Object> unlike(@PathVariable long reviewId, @RequestBody LikeReq req) {
        long likes = reviewService.unlikeReview(req.auth, reviewId);
        return Map.of("likes", likes);
    }

    @GetMapping("/recipes/{recipeId}/reviews")
    public PageResult<ReviewRecord> list(@PathVariable long recipeId,
                                         @RequestParam int page,
                                         @RequestParam int size,
                                         @RequestParam String sort) {
        return reviewService.listByRecipe(recipeId, page, size, sort);
    }

    @PostMapping("/recipes/{recipeId}/reviews/refresh-rating")
    public RecipeRecord refresh(@PathVariable long recipeId) {
        return reviewService.refreshRecipeAggregatedRating(recipeId);
    }
}
