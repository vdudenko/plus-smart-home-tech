package ru.yandex.practicum.analyzer.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "scenario_conditions")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class ScenarioCondition {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "scenario_id", nullable = false)
    private Scenario scenario;

    @Column(name = "sensor_id", nullable = false)
    private String sensorId;

    @ManyToOne
    @JoinColumn(name = "condition_id", nullable = false)
    private Condition condition;
}