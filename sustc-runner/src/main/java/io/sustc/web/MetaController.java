package io.sustc.web;

import io.sustc.service.DatabaseService;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@Profile("server")
@RequestMapping("/api/meta")
public class MetaController {

    private final DatabaseService databaseService;

    public MetaController(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @GetMapping("/group-members")
    public List<Integer> groupMembers() {
        return databaseService.getGroupMembers();
    }

    @GetMapping("/sum")
    public Map<String, Object> sum(@RequestParam int a, @RequestParam int b) {
        return Map.of("a", a, "b", b, "sum", databaseService.sum(a, b));
    }
}
