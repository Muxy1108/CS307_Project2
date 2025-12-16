package io.sustc.service.impl;

import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12410208,12412559);
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public void importData(
            List<ReviewRecord> reviewRecords,
            List<UserRecord> userRecords,
            List<RecipeRecord> recipeRecords) {

        createTables();
        log.info("db user = {}", jdbcTemplate.queryForObject("select current_user", String.class));
        log.info("db name = {}", jdbcTemplate.queryForObject("select current_database()", String.class));


        if (userRecords == null) userRecords = Collections.emptyList();
        if (recipeRecords == null) recipeRecords = Collections.emptyList();
        if (reviewRecords == null) reviewRecords = Collections.emptyList();

        batchInsertUsers(userRecords);
        batchInsertRecipes(recipeRecords);
        batchInsertRecipeIngredients(recipeRecords);
        batchInsertReviews(reviewRecords);
        batchInsertReviewLikes(reviewRecords);
        batchInsertUserFollows(userRecords);

        createIndexes();
    }


    private void batchInsertUsers(List<UserRecord> users) {
        if (users.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO users " +
                "  (AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (AuthorId) DO NOTHING;";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                UserRecord u = users.get(i);
                ps.setLong(1, u.getAuthorId());
                ps.setString(2, u.getAuthorName());
                ps.setString(3, u.getGender());
                ps.setObject(4, u.getAge());
                ps.setObject(5, u.getFollowers());
                ps.setObject(6, u.getFollowing());
                ps.setString(7, u.getPassword());
                ps.setObject(8, u.isDeleted());
            }

            @Override
            public int getBatchSize() {
                return users.size();
            }
        });
    }

    private void batchInsertRecipes(List<RecipeRecord> recipes) {
        if (recipes.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO recipes " +
                "  (RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, Description, " +
                "   RecipeCategory, AggregatedRating, ReviewCount, Calories, FatContent, SaturatedFatContent, " +
                "   CholesterolContent, SodiumContent, CarbohydrateContent, FiberContent, SugarContent, " +
                "   ProteinContent, RecipeServings, RecipeYield) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (RecipeId) DO NOTHING;";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RecipeRecord r = recipes.get(i);
                ps.setLong(1, r.getRecipeId());
                ps.setString(2, r.getName());
                ps.setLong(3, r.getAuthorId());
                ps.setString(4, r.getCookTime());
                ps.setString(5, r.getPrepTime());
                ps.setString(6, r.getTotalTime());
                ps.setTimestamp(7, r.getDatePublished());
                ps.setString(8, r.getDescription());
                ps.setString(9, r.getRecipeCategory());
                ps.setObject(10, r.getAggregatedRating());
                ps.setObject(11, r.getReviewCount());
                ps.setObject(12, r.getCalories());
                ps.setObject(13, r.getFatContent());
                ps.setObject(14, r.getSaturatedFatContent());
                ps.setObject(15, r.getCholesterolContent());
                ps.setObject(16, r.getSodiumContent());
                ps.setObject(17, r.getCarbohydrateContent());
                ps.setObject(18, r.getFiberContent());
                ps.setObject(19, r.getSugarContent());
                ps.setObject(20, r.getProteinContent());
                ps.setObject(21, r.getRecipeServings());
                ps.setObject(22, r.getRecipeYield());
            }

            @Override
            public int getBatchSize() {
                return recipes.size();
            }
        });
    }

    private void batchInsertRecipeIngredients(List<RecipeRecord> recipes) {
        if (recipes.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) " +
                "VALUES (?, ?) " +
                "ON CONFLICT DO NOTHING;";

        List<Object[]> ingredientPairs = new ArrayList<>();
        for (RecipeRecord recipe : recipes) {
            if (recipe.getRecipeIngredientParts() == null) {
                continue;
            }
            for (String ingredient : recipe.getRecipeIngredientParts()) {
                ingredientPairs.add(new Object[]{recipe.getRecipeId(), ingredient});
            }
        }

        if (ingredientPairs.isEmpty()) {
            return;
        }

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Object[] pair = ingredientPairs.get(i);
                ps.setLong(1, (Long) pair[0]);
                ps.setString(2, (String) pair[1]);
            }

            @Override
            public int getBatchSize() {
                return ingredientPairs.size();
            }
        });
    }

    private void batchInsertReviews(List<ReviewRecord> reviews) {
        if (reviews.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO reviews " +
                "  (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (ReviewId) DO NOTHING;";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ReviewRecord r = reviews.get(i);
                ps.setLong(1, r.getReviewId());
                ps.setLong(2, r.getRecipeId());
                ps.setLong(3, r.getAuthorId());
                ps.setObject(4, r.getRating());
                ps.setString(5, r.getReview());
                ps.setTimestamp(6, r.getDateSubmitted());
                ps.setTimestamp(7, r.getDateModified());
            }

            @Override
            public int getBatchSize() {
                return reviews.size();
            }
        });
    }

    private void batchInsertReviewLikes(List<ReviewRecord> reviews) {
        if (reviews.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO review_likes (ReviewId, AuthorId) " +
                "VALUES (?, ?) " +
                "ON CONFLICT DO NOTHING;";

        List<long[]> likePairs = new ArrayList<>();
        for (ReviewRecord r : reviews) {
            long[] likes = r.getLikes();
            if (likes == null || likes.length == 0) {
                continue;
            }
            for (long userId : likes) {
                likePairs.add(new long[]{r.getReviewId(), userId});
            }
        }


        if (likePairs.isEmpty()) {
            return;
        }

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                long[] pair = likePairs.get(i);
                ps.setLong(1, pair[0]);
                ps.setLong(2, pair[1]);
            }

            @Override
            public int getBatchSize() {
                return likePairs.size();
            }
        });
    }

    private void batchInsertUserFollows(List<UserRecord> users) {
        if (users.isEmpty()) {
            return;
        }

        String sql = "INSERT INTO user_follows (FollowerId, FollowingId) " +
                "VALUES (?, ?) " +
                "ON CONFLICT DO NOTHING;";

        List<long[]> followPairs = new ArrayList<>();
        for (UserRecord u : users) {
            long[] followers = u.getFollowerUsers();
            if (followers != null) {
                for (long follower : followers) {
                    followPairs.add(new long[]{follower, u.getAuthorId()});
                }
            }

            long[] followings = u.getFollowingUsers();
            if (followings != null) {
                for (long following : followings) {
                    followPairs.add(new long[]{u.getAuthorId(), following});
                }
            }
        }


        if (followPairs.isEmpty()) {
            return;
        }

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                long[] pair = followPairs.get(i);
                ps.setLong(1, pair[0]);
                ps.setLong(2, pair[1]);
            }

            @Override
            public int getBatchSize() {
                return followPairs.size();
            }
        });
    }

    private void createIndexes(){
        String[] indexSqls = new String[]{
                "CREATE INDEX IF NOT EXISTS idx_recipes_author ON recipes (AuthorId);",
                "CREATE INDEX IF NOT EXISTS idx_reviews_recipe ON reviews (RecipeId);",
                "CREATE INDEX IF NOT EXISTS idx_reviews_author ON reviews (AuthorId);",
                "CREATE INDEX IF NOT EXISTS idx_review_likes_review ON review_likes (ReviewId);",
                "CREATE INDEX IF NOT EXISTS idx_review_likes_author ON review_likes (AuthorId);",
                "CREATE INDEX IF NOT EXISTS idx_user_follows_follower ON user_follows (FollowerId);",
                "CREATE INDEX IF NOT EXISTS idx_user_follows_following ON user_follows (FollowingId);"};

        for (String sql : indexSqls) {
            jdbcTemplate.execute(sql);
        }
    }


    private void createTables() {
        String[] createTableSQLs = {
                // 创建users表
                "CREATE TABLE IF NOT EXISTS users (" +
                        "    AuthorId BIGINT PRIMARY KEY, " +
                        "    AuthorName VARCHAR(255) NOT NULL, " +
                        "    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')), " +
                        "    Age INTEGER CHECK (Age > 0), " +
                        "    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0), " +
                        "    Following INTEGER DEFAULT 0 CHECK (Following >= 0), " +
                        "    Password VARCHAR(255), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",

                // 创建recipes表
                "CREATE TABLE IF NOT EXISTS recipes (" +
                        "    RecipeId BIGINT PRIMARY KEY, " +
                        "    Name VARCHAR(500) NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    CookTime VARCHAR(50), " +
                        "    PrepTime VARCHAR(50), " +
                        "    TotalTime VARCHAR(50), " +
                        "    DatePublished TIMESTAMP, " +
                        "    Description TEXT, " +
                        "    RecipeCategory VARCHAR(255), " +
                        "    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
                        "    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0), " +
                        "    Calories DECIMAL(10,2), " +
                        "    FatContent DECIMAL(10,2), " +
                        "    SaturatedFatContent DECIMAL(10,2), " +
                        "    CholesterolContent DECIMAL(10,2), " +
                        "    SodiumContent DECIMAL(10,2), " +
                        "    CarbohydrateContent DECIMAL(10,2), " +
                        "    FiberContent DECIMAL(10,2), " +
                        "    SugarContent DECIMAL(10,2), " +
                        "    ProteinContent DECIMAL(10,2), " +
                        "    RecipeServings VARCHAR(100), " +
                        "    RecipeYield VARCHAR(100), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建reviews表
                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating FLOAT, " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建recipe_ingredients表
                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId)" +
                        ")",

                // 创建review_likes表
                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建user_follows表
                "CREATE TABLE IF NOT EXISTS user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId), " +
                        "    CHECK (FollowerId != FollowingId)" +
                        ")"
        };

        for (String sql : createTableSQLs) {
            jdbcTemplate.execute(sql);
        }
    }



    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void drop() {
        // You can use the default drop script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.
        // This method will delete all the tables in the public schema.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (java.sql.Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (java.sql.Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
