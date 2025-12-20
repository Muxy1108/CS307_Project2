package io.sustc.web;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@Profile("server")
@RequestMapping("/api/users")
public class UserController {

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping("/register")
    public Map<String, Object> register(@RequestBody RegisterUserReq req) {
        long id = userService.register(req);
        return Map.of("authorId", id);
    }

    @PostMapping("/login")
    public Map<String, Object> login(@RequestBody AuthInfo auth) {
        long id = userService.login(auth);
        return Map.of("authorId", id);
    }

    @GetMapping("/{userId}")
    public UserRecord get(@PathVariable long userId) {
        return userService.getById(userId);
    }

    public static class UpdateProfileReq {
        public AuthInfo auth;
        public String gender;
        public Integer age;
    }

    @PatchMapping("/profile")
    public Map<String, Object> updateProfile(@RequestBody UpdateProfileReq req) {
        userService.updateProfile(req.auth, req.gender, req.age);
        return Map.of("ok", true);
    }

    public static class DeleteAccountReq {
        public AuthInfo auth;
    }

    @DeleteMapping("/{userId}")
    public Map<String, Object> deleteAccount(@PathVariable long userId, @RequestBody DeleteAccountReq req) {
        boolean ok = userService.deleteAccount(req.auth, userId);
        return Map.of("ok", ok);
    }

    public static class FollowReq {
        public AuthInfo auth;
    }

    @PostMapping("/follow/{followeeId}")
    public Map<String, Object> followToggle(@PathVariable long followeeId, @RequestBody FollowReq req) {
        boolean following = userService.follow(req.auth, followeeId);
        return Map.of("following", following);
    }

    @GetMapping("/feed")
    public PageResult<FeedItem> feed(@RequestBody AuthInfo auth,
                                     @RequestParam int page,
                                     @RequestParam int size,
                                     @RequestParam(required = false) String category) {
        return userService.feed(auth, page, size, category);
    }

    @GetMapping("/analytics/highest-follow-ratio")
    public Map<String, Object> highestFollowRatio() {
        return userService.getUserWithHighestFollowRatio();
    }
}
