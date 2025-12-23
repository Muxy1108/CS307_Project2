package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.RecipeService;
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
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@Slf4j
public class RecipeServiceImpl implements RecipeService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 只负责把一行 recipe + authorName 映射成 RecipeRecord，
     * 不负责填充 recipeIngredientParts（之后统一在 Java 里补上）。
     */
    private final RowMapper<RecipeRecord> recipeRowMapper = new RowMapper<>() {
        @Override
        public RecipeRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            RecipeRecord.RecipeRecordBuilder builder = RecipeRecord.builder();
            builder.RecipeId(rs.getLong("recipeid"));
            builder.name(rs.getString("name"));
            builder.authorId(rs.getLong("authorid"));
            // 主查询里 JOIN users u 之后，这里就能拿到作者名
            builder.authorName(rs.getString("authorname"));

            builder.cookTime(rs.getString("cooktime"));
            builder.prepTime(rs.getString("preptime"));
            builder.totalTime(rs.getString("totaltime"));
            builder.datePublished(rs.getTimestamp("datepublished"));
            builder.description(rs.getString("description"));
            builder.recipeCategory(rs.getString("recipecategory"));

            // ingredients 之后统一补，这里先设为空
            builder.recipeIngredientParts(null);

            java.math.BigDecimal ar = rs.getBigDecimal("aggregatedrating");
            builder.aggregatedRating(ar == null ? 0f : ar.floatValue());

            Integer rc = rs.getObject("reviewcount", Integer.class);
            builder.reviewCount(rc == null ? 0 : rc);

            builder.calories(getDecimalAsFloat(rs, "calories"));
            builder.fatContent(getDecimalAsFloat(rs, "fatcontent"));
            builder.saturatedFatContent(getDecimalAsFloat(rs, "saturatedfatcontent"));
            builder.cholesterolContent(getDecimalAsFloat(rs, "cholesterolcontent"));
            builder.sodiumContent(getDecimalAsFloat(rs, "sodiumcontent"));
            builder.carbohydrateContent(getDecimalAsFloat(rs, "carbohydratecontent"));
            builder.fiberContent(getDecimalAsFloat(rs, "fibercontent"));
            builder.sugarContent(getDecimalAsFloat(rs, "sugarcontent"));
            builder.proteinContent(getDecimalAsFloat(rs, "proteincontent"));

            Integer servings = null;
            try {
                servings = rs.getObject("recipeservings", Integer.class);
            } catch (SQLException ignored) {
                // ignore parsing errors
            }
            if (servings == null) {
                String servingsStr = rs.getString("recipeservings");
                if (servingsStr != null) {
                    try {
                        servings = (int) Double.parseDouble(servingsStr.trim());
                    } catch (NumberFormatException ignored) {
                        servings = 0;
                    }
                }
            }
            builder.recipeServings(servings == null ? 0 : servings);
            builder.recipeYield(rs.getString("recipeyield"));

            return builder.build();
        }

        private float getDecimalAsFloat(ResultSet rs, String column) throws SQLException {
            java.math.BigDecimal bd = rs.getBigDecimal(column);
            return bd == null ? 0f : bd.floatValue();
        }
    };

    @Override
    public String getNameFromID(long id) {
        if (id <= 0) {
            throw new IllegalArgumentException("Invalid recipe id");
        }
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT name FROM recipes WHERE recipeid = ?",
                    String.class,
                    id
            );
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public RecipeRecord getRecipeById(long recipeId) {
        if (recipeId <= 0) {
            throw new IllegalArgumentException("Invalid recipe id");
        }
        try {
            // 这里直接 JOIN users 拿到 authorName
            RecipeRecord record = jdbcTemplate.queryForObject(
                    "SELECT r.*, u.authorname " +
                            "FROM recipes r " +
                            "JOIN users u ON r.authorid = u.authorid " +
                            "WHERE r.recipeid = ?",
                    recipeRowMapper,
                    recipeId
            );
            if (record == null) {
                return null;
            }
            // 单条记录的 ingredients 单独查一条 SQL 即可，开销可以忽略
            List<String> ingredients = jdbcTemplate.query(
                    "SELECT ingredientpart FROM recipe_ingredients " +
                            "WHERE recipeid = ? " +
                            "ORDER BY ingredientpart ASC",
                    (rs, rowNum) -> rs.getString("ingredientpart"),
                    recipeId
            );
            record.setRecipeIngredientParts(ingredients.toArray(new String[0]));
            return record;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating,
                                                  Integer page, Integer size, String sort) {
        if (page == null || page < 1 || size == null || size <= 0) {
            throw new IllegalArgumentException("Invalid page or size");
        }

        StringBuilder where = new StringBuilder(" WHERE 1=1");
        List<Object> params = new ArrayList<>();

        // 注意都加上 r. 前缀
        if (StringUtils.hasText(keyword)) {
            where.append(" AND (r.name ILIKE ? OR r.description ILIKE ?)");
            String kw = "%" + keyword + "%";
            params.add(kw);
            params.add(kw);
        }
        if (StringUtils.hasText(category)) {
            where.append(" AND r.recipecategory = ?");
            params.add(category);
        }
        if (minRating != null) {
            where.append(" AND r.aggregatedrating >= ?");
            params.add(minRating);
        }

        String orderBy;
        if ("rating_desc".equalsIgnoreCase(sort)) {
            // 先评分降序，再发布日期降序，最后 recipeid 升序
            orderBy = " ORDER BY aggregatedrating DESC NULLS LAST, " +
                    "datepublished DESC NULLS LAST, recipeid ASC";
        } else if ("date_desc".equalsIgnoreCase(sort)) {
            orderBy = " ORDER BY datepublished DESC NULLS LAST, recipeid ASC";
        } else if ("calories_asc".equalsIgnoreCase(sort)) {
            orderBy = " ORDER BY calories ASC NULLS LAST, recipeid ASC";
        } else {
            orderBy = " ORDER BY recipeid ASC";
        }


        String fromClause = " FROM recipes r JOIN users u ON r.authorid = u.authorid";

        // 统计总数
        long total = Objects.requireNonNull(jdbcTemplate.queryForObject(
                "SELECT COUNT(*)" + fromClause + where,
                Long.class,
                params.toArray()
        ));

        // 查当前页的 recipe + authorName
        int offset = (page - 1) * size;
        String pageSql =
                "SELECT r.*, u.authorname" +
                        fromClause +
                        where +
                        orderBy +
                        " LIMIT ? OFFSET ?";

        List<Object> pageParams = new ArrayList<>(params);
        pageParams.add(size);
        pageParams.add(offset);

        List<RecipeRecord> items = jdbcTemplate.query(
                pageSql,
                recipeRowMapper,
                pageParams.toArray()
        );

        // 一次性把这一页所有 recipe 的 ingredients 查出来并填回去
        fillIngredientsForRecipes(items);

        return PageResult.<RecipeRecord>builder()
                .items(items)
                .page(page)
                .size(size)
                .total(total)
                .build();
    }

    @Override
    @Transactional
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        long authorId = requireActiveUser(auth);

        if (dto == null || !StringUtils.hasText(dto.getName())) {
            return -1;
        }

        // If caller provides id, use it; otherwise allocate via sequence (recommended)
        Long recipeId = (dto.getRecipeId() > 0) ? dto.getRecipeId()
                : jdbcTemplate.queryForObject("SELECT nextval('recipe_id_seq')", Long.class);

        Timestamp datePublished = (dto.getDatePublished() != null)
                ? dto.getDatePublished()
                : new Timestamp(System.currentTimeMillis());


        // Insert; if conflict happens, RETURNING yields 0 rows -> return -1 (no exception)
        List<Long> inserted = jdbcTemplate.query(
                "INSERT INTO recipes (recipeid, name, authorid, cooktime, preptime, totaltime, " +
                        "datepublished, description, recipecategory, aggregatedrating, reviewcount, " +
                        "calories, fatcontent, saturatedfatcontent, cholesterolcontent, sodiumcontent, " +
                        "carbohydratecontent, fibercontent, sugarcontent, proteincontent, " +
                        "recipeservings, recipeyield) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
                        "ON CONFLICT (recipeid) DO NOTHING " +
                        "RETURNING recipeid",
                (rs, rn) -> rs.getLong(1),
                recipeId,
                dto.getName().trim(),
                authorId,
                dto.getCookTime(),
                dto.getPrepTime(),
                dto.getTotalTime(),
                datePublished,          // keep null if null
                dto.getDescription(),
                dto.getRecipeCategory(),
                0.0,                             // safer than NULL for filtering
                0,
                dto.getCalories(),
                dto.getFatContent(),
                dto.getSaturatedFatContent(),
                dto.getCholesterolContent(),
                dto.getSodiumContent(),
                dto.getCarbohydrateContent(),
                dto.getFiberContent(),
                dto.getSugarContent(),
                dto.getProteinContent(),
                dto.getRecipeServings(),
                dto.getRecipeYield()
        );

        if (inserted.isEmpty()) {
            return -1;
        }

        // Insert ingredients only if recipe insert succeeded
        String[] parts = dto.getRecipeIngredientParts();
        if (parts != null && parts.length > 0) {
            List<Object[]> batch = new ArrayList<>();
            for (String p : parts) {
                if (StringUtils.hasText(p)) batch.add(new Object[]{recipeId, p});
            }
            if (!batch.isEmpty()) {
                jdbcTemplate.batchUpdate(
                        "INSERT INTO recipe_ingredients (recipeid, ingredientpart) VALUES (?, ?)" +
                                "ON CONFLICT DO NOTHING",
                        batch
                );
            }
        }

        return recipeId;
    }



    @Override
    @Transactional
    public void deleteRecipe(long recipeId, AuthInfo auth) {
        if (recipeId <= 0) {
            throw new IllegalArgumentException("Invalid recipe id");
        }
        long userId = requireActiveUser(auth);
        Long authorId;
        try {
            authorId = jdbcTemplate.queryForObject(
                    "SELECT authorid FROM recipes WHERE recipeid = ?",
                    Long.class,
                    recipeId
            );
        } catch (EmptyResultDataAccessException e) {
            return;
        }
        if (authorId == null || authorId != userId) {
            throw new SecurityException("Not authorized to delete");
        }

        jdbcTemplate.update("DELETE FROM reviews WHERE recipeid = ?", recipeId);
        jdbcTemplate.update("DELETE FROM recipe_ingredients WHERE recipeid = ?", recipeId);
        jdbcTemplate.update("DELETE FROM recipes WHERE recipeid = ?", recipeId);
    }

    @Override
    @Transactional
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {

        if (cookTimeIso == null && prepTimeIso == null) {
            return;
        }

        if (recipeId <= 0) {
            throw new IllegalArgumentException("Invalid recipe id");
        }
        long userId = requireActiveUser(auth);
        Map<String, Object> recipe;
        try {
            recipe = jdbcTemplate.queryForMap(
                    "SELECT authorid, cooktime, preptime FROM recipes WHERE recipeid = ?",
                    recipeId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Recipe not found");
        }
        Long authorId = ((Number) recipe.get("authorid")).longValue();
        if (authorId != userId) {
            throw new SecurityException("Not authorized to update");
        }

        String currentCook = (String) recipe.get("cooktime");
        String currentPrep = (String) recipe.get("preptime");

        String finalCook = cookTimeIso != null ? cookTimeIso : currentCook;
        String finalPrep = prepTimeIso != null ? prepTimeIso : currentPrep;

        Duration cookDuration = parseDuration(finalCook);
        Duration prepDuration = parseDuration(finalPrep);

        Duration totalDuration = cookDuration.plus(prepDuration);
        if (totalDuration.isNegative()) {
            throw new IllegalArgumentException("Negative duration");
        }

        String totalIso = totalDuration.toString();
        jdbcTemplate.update(
                "UPDATE recipes SET cooktime = ?, preptime = ?, totaltime = ? WHERE recipeid = ?",
                finalCook,
                finalPrep,
                totalIso,
                recipeId
        );
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        String sql =
                "WITH ordered AS (" +
                        "    SELECT recipeid, calories, " +
                        "           LAG(recipeid) OVER (ORDER BY calories ASC, recipeid ASC) AS prev_id, " +
                        "           LAG(calories) OVER (ORDER BY calories ASC, recipeid ASC) AS prev_cal " +
                        "    FROM recipes WHERE calories IS NOT NULL" +
                        "), diffs AS (" +
                        "    SELECT CASE WHEN recipeid < prev_id THEN recipeid ELSE prev_id END AS recipe_a, " +
                        "           CASE WHEN recipeid < prev_id THEN prev_id ELSE recipeid END AS recipe_b, " +
                        "           CASE WHEN recipeid < prev_id THEN calories ELSE prev_cal END AS calories_a, " +
                        "           CASE WHEN recipeid < prev_id THEN prev_cal ELSE calories END AS calories_b, " +
                        "           ABS(calories - prev_cal) AS diff " +
                        "    FROM ordered WHERE prev_id IS NOT NULL" +
                        ") " +
                        "SELECT recipe_a, recipe_b, calories_a, calories_b, diff FROM diffs " +
                        "ORDER BY diff ASC, recipe_a ASC, recipe_b ASC LIMIT 1";
        try {
            Map<String, Object> row = jdbcTemplate.queryForMap(sql);
            Map<String, Object> result = new HashMap<>();
            Long recipeA = ((Number) row.get("recipe_a")).longValue();
            Long recipeB = ((Number) row.get("recipe_b")).longValue();
            Double caloriesA = row.get("calories_a") == null ? null : ((Number) row.get("calories_a")).doubleValue();
            Double caloriesB = row.get("calories_b") == null ? null : ((Number) row.get("calories_b")).doubleValue();
            Double diff = row.get("diff") == null ? null : ((Number) row.get("diff")).doubleValue();
            result.put("RecipeA", recipeA);
            result.put("RecipeB", recipeB);
            result.put("CaloriesA", caloriesA);
            result.put("CaloriesB", caloriesB);
            result.put("Difference", diff);
            return result;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        String sql =
                "SELECT ri.recipeid, r.name, COUNT(DISTINCT ri.ingredientpart) AS ingredient_count " +
                        "FROM recipe_ingredients ri " +
                        "JOIN recipes r ON ri.recipeid = r.recipeid " +
                        "GROUP BY ri.recipeid, r.name " +
                        "ORDER BY ingredient_count DESC, ri.recipeid ASC " +
                        "LIMIT 3";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Map<String, Object> item = new HashMap<>();
            item.put("RecipeId", ((Number) row.get("recipeid")).longValue());
            item.put("Name", row.get("name"));
            item.put("IngredientCount", ((Number) row.get("ingredient_count")).intValue());
            result.add(item);
        }
        return result;
    }

    // ====================== 私有工具方法 ======================

    private long generateRecipeId() {
        try {
            Long id = jdbcTemplate.queryForObject(
                    "SELECT COALESCE(MAX(recipeid),0)+1 FROM recipes FOR UPDATE",
                    Long.class
            );
            return id == null ? 1L : id;
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate recipe id", e);
        }
    }

    private long requireActiveUser(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0 || !StringUtils.hasText(auth.getPassword())) {
            throw new SecurityException("Invalid auth");
        }
        try {
            Map<String, Object> row = jdbcTemplate.queryForMap(
                    "SELECT authorid, password, isdeleted FROM users WHERE authorid = ?",
                    auth.getAuthorId()
            );
            if (Boolean.TRUE.equals(row.get("isdeleted"))) {
                throw new SecurityException("Inactive user");
            }
            String dbPwd = (String) row.get("password");
            if (dbPwd == null || !dbPwd.equals(auth.getPassword())) {
                throw new SecurityException("Wrong password");
            }
            return ((Number) row.get("authorid")).longValue();
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("User not found");
        }
    }


    private Duration parseDuration(String iso) {
        // 空字符串、全是空格、一律视为 0 时长
        if (!StringUtils.hasText(iso)) {
            return Duration.ZERO;
        }
        try {
            Duration d = Duration.parse(iso.trim());
            if (d.isNegative()) {
                throw new IllegalArgumentException("Negative duration");
            }
            return d;
        } catch (DateTimeParseException e) {
            // 真正的非法格式再抛异常
            throw new IllegalArgumentException("Invalid duration format", e);
        }
    }


    /**
     * 一次性把一页 recipe 的所有配料查出来并填回到 RecipeRecord 中
     */
    private void fillIngredientsForRecipes(List<RecipeRecord> recipes) {
        if (recipes == null || recipes.isEmpty()) {
            return;
        }

        // 收集这一页所有 recipeId
        List<Long> ids = new ArrayList<>(recipes.size());
        for (RecipeRecord r : recipes) {
            ids.add(r.getRecipeId());
        }

        // 构造 IN (?, ?, ?, ...)
        StringBuilder inClause = new StringBuilder();
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                inClause.append(",");
            }
            inClause.append("?");
        }

        String sql =
                "SELECT recipeid, ingredientpart " +
                        "FROM recipe_ingredients " +
                        "WHERE recipeid IN (" + inClause + ") " +
                        "ORDER BY recipeid, ingredientpart";

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql, ids.toArray());

        // 把 ingredient 按 recipeid 分组
        Map<Long, List<String>> ingredientMap = new HashMap<>();
        for (Map<String, Object> row : rows) {
            Long rid = ((Number) row.get("recipeid")).longValue();
            String part = (String) row.get("ingredientpart");
            ingredientMap.computeIfAbsent(rid, k -> new ArrayList<>()).add(part);
        }

        // 填回到每个 RecipeRecord
        for (RecipeRecord r : recipes) {
            List<String> list = ingredientMap.get(r.getRecipeId());
            if (list != null) {
                r.setRecipeIngredientParts(list.toArray(new String[0]));
            } else {
                r.setRecipeIngredientParts(new String[0]);
            }
        }
    }
}
