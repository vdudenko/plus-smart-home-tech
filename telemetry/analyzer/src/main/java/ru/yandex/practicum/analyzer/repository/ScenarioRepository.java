package ru.yandex.practicum.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.analyzer.entity.Scenario;

import java.util.List;
import java.util.Optional;

@Repository
public interface ScenarioRepository extends JpaRepository<Scenario, Long> {
    List<Scenario> findByHubId(String hubId);

    Optional<Scenario> findByHubIdAndName(String hubId, String name);

    @Query("SELECT s FROM Scenario s " +
            "LEFT JOIN FETCH s.conditions c " +
            "LEFT JOIN FETCH c.condition " +
            "LEFT JOIN FETCH s.actions a " +
            "LEFT JOIN FETCH a.action " +
            "WHERE s.hubId = :hubId")
    List<Scenario> findByHubIdWithConditionsAndActions(String hubId);
}