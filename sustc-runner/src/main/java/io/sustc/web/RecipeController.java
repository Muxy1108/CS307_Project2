package io.sustc.web;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.RecipeService;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

        import java.util.List;
import java.util.Map;

@RestController
@Profile("server")
@RequestMapping("/api/recipes")
public class RecipeController {

    private final RecipeService recipeService;

    public RecipeController(RecipeService recipeService) {
        this.recipeService = recipeService;
    }

    @GetMapping("/{recipeId}")
    public ResponseEntity<RecipeRecord> getById(@PathVariable long recipeId) {
        RecipeRecord r = recipeService.getRecipeById(recipeId);
        if (r == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(r);
    }

    @GetMapping("/search")
    public PageResult<RecipeRecord> search(
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) Double minRating,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false, defaultValue = "datePublished:desc") String sort
    ) {
        return recipeService.searchRecipes(keyword, category, minRating, page, size, sort);
    }

    public static class CreateRecipeReq {
        public RecipeRecord dto;
        public AuthInfo auth;
    }

    @PostMapping
    public Map<String, Object> create(@RequestBody CreateRecipeReq req) {
        long id = recipeService.createRecipe(req.dto, req.auth);
        return Map.of("recipeId", id);
    }

    public static class UpdateTimesReq {
        public AuthInfo auth;
        public String cookTimeIso;
        public String prepTimeIso;
    }

    @PatchMapping("/{recipeId}/times")
    public Map<String, Object> updateTimes(@PathVariable long recipeId, @RequestBody UpdateTimesReq req) {
        recipeService.updateTimes(req.auth, recipeId, req.cookTimeIso, req.prepTimeIso);
        return Map.of("ok", true);
    }

    public static class DeleteRecipeReq {
        public AuthInfo auth;
    }

    @DeleteMapping("/{recipeId}")
    public Map<String, Object> delete(@PathVariable long recipeId, @RequestBody DeleteRecipeReq req) {
        recipeService.deleteRecipe(recipeId, req.auth);
        return Map.of("ok", true);
    }

    @GetMapping("/analytics/closest-calorie-pair")
    public Map<String, Object> closestCaloriePair() {
        return recipeService.getClosestCaloriePair();
    }

    @GetMapping("/analytics/top3-complex-by-ingredients")
    public List<Map<String, Object>> top3Complex() {
        return recipeService.getTop3MostComplexRecipesByIngredients();
    }
}
