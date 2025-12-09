package io.sustc.service.impl;

import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.service.DatabaseService;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implementation of {@link DatabaseService} using plain JDBC.
 */
public class DatabaseServiceImpl implements DatabaseService {

    private static final String URL = "jdbc:postgresql://localhost:5432/sustc";
    private static final String USER = "sustc";
    private static final String PASSWORD = "sustec";
    private static final int BATCH_SIZE = 1000;

    private Connection getConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12211708, 12211709);
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ? + ? AS s";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("s");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void importData(List<ReviewRecord> reviewRecords, List<UserRecord> userRecords, List<RecipeRecord> recipeRecords) {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            createTables(conn);
            importUsers(conn, userRecords);
            importRecipes(conn, recipeRecords);
            importReviews(conn, reviewRecords);
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void createTables(Connection conn) throws SQLException {
        String[] sqls = new String[]{
                "CREATE TABLE IF NOT EXISTS users (" +
                        "    AuthorId BIGINT PRIMARY KEY, " +
                        "    AuthorName VARCHAR(255) NOT NULL, " +
                        "    Gender VARCHAR(10), " +
                        "    Age INTEGER, " +
                        "    Followers INTEGER DEFAULT 0, " +
                        "    Following INTEGER DEFAULT 0, " +
                        "    Password VARCHAR(255), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",
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
                        "    AggregatedRating DECIMAL(4,2), " +
                        "    ReviewCount INTEGER, " +
                        "    Calories DECIMAL(10,2), " +
                        "    FatContent DECIMAL(10,2), " +
                        "    SaturatedFatContent DECIMAL(10,2), " +
                        "    CholesterolContent DECIMAL(10,2), " +
                        "    SodiumContent DECIMAL(10,2), " +
                        "    CarbohydrateContent DECIMAL(10,2), " +
                        "    FiberContent DECIMAL(10,2), " +
                        "    SugarContent DECIMAL(10,2), " +
                        "    ProteinContent DECIMAL(10,2), " +
                        "    RecipeServings INTEGER, " +
                        "    RecipeYield VARCHAR(100), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating DECIMAL(4,2), " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",
                "CREATE TABLE IF NOT EXISTS user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId)" +
                        ")"
        };

        try (Statement stmt = conn.createStatement()) {
            for (String sql : sqls) {
                stmt.execute(sql);
            }
        }
    }

    private void importUsers(Connection conn, List<UserRecord> users) throws SQLException {
        String userSql = "INSERT INTO users (AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        String followSql = "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)";

        try (PreparedStatement userStmt = conn.prepareStatement(userSql);
             PreparedStatement followStmt = conn.prepareStatement(followSql)) {
            int count = 0;
            for (UserRecord user : users) {
                userStmt.setLong(1, user.getAuthorId());
                userStmt.setString(2, user.getAuthorName());
                userStmt.setString(3, user.getGender());
                userStmt.setInt(4, user.getAge());
                userStmt.setInt(5, user.getFollowers());
                userStmt.setInt(6, user.getFollowing());
                userStmt.setString(7, user.getPassword());
                userStmt.setBoolean(8, user.isDeleted());
                userStmt.addBatch();
                count++;
                if (count % BATCH_SIZE == 0) {
                    userStmt.executeBatch();
                }
            }
            userStmt.executeBatch();

            Set<String> followPairs = new HashSet<>();
            count = 0;
            for (UserRecord user : users) {
                if (user.getFollowingUsers() != null) {
                    for (long follow : user.getFollowingUsers()) {
                        String key = user.getAuthorId() + "-" + follow;
                        if (followPairs.add(key)) {
                            followStmt.setLong(1, user.getAuthorId());
                            followStmt.setLong(2, follow);
                            followStmt.addBatch();
                            count++;
                            if (count % BATCH_SIZE == 0) {
                                followStmt.executeBatch();
                            }
                        }
                    }
                }
                if (user.getFollowerUsers() != null) {
                    for (long follower : user.getFollowerUsers()) {
                        String key = follower + "-" + user.getAuthorId();
                        if (followPairs.add(key)) {
                            followStmt.setLong(1, follower);
                            followStmt.setLong(2, user.getAuthorId());
                            followStmt.addBatch();
                            count++;
                            if (count % BATCH_SIZE == 0) {
                                followStmt.executeBatch();
                            }
                        }
                    }
                }
            }
            followStmt.executeBatch();
        }
    }

    private void importRecipes(Connection conn, List<RecipeRecord> recipes) throws SQLException {
        String recipeSql = "INSERT INTO recipes (RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, Description, " +
                "RecipeCategory, AggregatedRating, ReviewCount, Calories, FatContent, SaturatedFatContent, CholesterolContent, SodiumContent, " +
                "CarbohydrateContent, FiberContent, SugarContent, ProteinContent, RecipeServings, RecipeYield) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String ingredientSql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?)";

        try (PreparedStatement recipeStmt = conn.prepareStatement(recipeSql);
             PreparedStatement ingredientStmt = conn.prepareStatement(ingredientSql)) {
            int count = 0;
            int ingredientCount = 0;
            for (RecipeRecord recipe : recipes) {
                recipeStmt.setLong(1, recipe.getRecipeId());
                recipeStmt.setString(2, recipe.getName());
                recipeStmt.setLong(3, recipe.getAuthorId());
                recipeStmt.setString(4, recipe.getCookTime());
                recipeStmt.setString(5, recipe.getPrepTime());
                recipeStmt.setString(6, recipe.getTotalTime());
                recipeStmt.setTimestamp(7, recipe.getDatePublished());
                recipeStmt.setString(8, recipe.getDescription());
                recipeStmt.setString(9, recipe.getRecipeCategory());
                recipeStmt.setFloat(10, recipe.getAggregatedRating());
                recipeStmt.setInt(11, recipe.getReviewCount());
                recipeStmt.setFloat(12, recipe.getCalories());
                recipeStmt.setFloat(13, recipe.getFatContent());
                recipeStmt.setFloat(14, recipe.getSaturatedFatContent());
                recipeStmt.setFloat(15, recipe.getCholesterolContent());
                recipeStmt.setFloat(16, recipe.getSodiumContent());
                recipeStmt.setFloat(17, recipe.getCarbohydrateContent());
                recipeStmt.setFloat(18, recipe.getFiberContent());
                recipeStmt.setFloat(19, recipe.getSugarContent());
                recipeStmt.setFloat(20, recipe.getProteinContent());
                recipeStmt.setInt(21, recipe.getRecipeServings());
                recipeStmt.setString(22, recipe.getRecipeYield());
                recipeStmt.addBatch();
                count++;
                if (count % BATCH_SIZE == 0) {
                    recipeStmt.executeBatch();
                }

                if (recipe.getRecipeIngredientParts() != null) {
                    for (String ingredient : recipe.getRecipeIngredientParts()) {
                        ingredientStmt.setLong(1, recipe.getRecipeId());
                        ingredientStmt.setString(2, ingredient);
                        ingredientStmt.addBatch();
                        ingredientCount++;
                        if (ingredientCount % BATCH_SIZE == 0) {
                            ingredientStmt.executeBatch();
                        }
                    }
                }
            }
            recipeStmt.executeBatch();
            ingredientStmt.executeBatch();
        }
    }

    private void importReviews(Connection conn, List<ReviewRecord> reviews) throws SQLException {
        String reviewSql = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        String likeSql = "INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?)";

        try (PreparedStatement reviewStmt = conn.prepareStatement(reviewSql);
             PreparedStatement likeStmt = conn.prepareStatement(likeSql)) {
            int count = 0;
            int likeCount = 0;
            for (ReviewRecord review : reviews) {
                reviewStmt.setLong(1, review.getReviewId());
                reviewStmt.setLong(2, review.getRecipeId());
                reviewStmt.setLong(3, review.getAuthorId());
                reviewStmt.setFloat(4, review.getRating());
                reviewStmt.setString(5, review.getReview());
                reviewStmt.setTimestamp(6, review.getDateSubmitted());
                reviewStmt.setTimestamp(7, review.getDateModified());
                reviewStmt.addBatch();
                count++;
                if (count % BATCH_SIZE == 0) {
                    reviewStmt.executeBatch();
                }

                if (review.getLikes() != null) {
                    for (long liker : review.getLikes()) {
                        likeStmt.setLong(1, review.getReviewId());
                        likeStmt.setLong(2, liker);
                        likeStmt.addBatch();
                        likeCount++;
                        if (likeCount % BATCH_SIZE == 0) {
                            likeStmt.executeBatch();
                        }
                    }
                }
            }
            reviewStmt.executeBatch();
            likeStmt.executeBatch();
        }
    }

    @Override
    public void drop() {
        String dropSchema = "DROP SCHEMA public CASCADE;";
        String createSchema = "CREATE SCHEMA public;";
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.execute(dropSchema);
            stmt.execute(createSchema);
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
