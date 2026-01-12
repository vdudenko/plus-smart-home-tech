package ru.yandex.practicum.analyzer.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "scenario_actions")
@Getter
@Setter  // ← Эта аннотация генерирует setScenario() автоматически
@NoArgsConstructor
@AllArgsConstructor
public class ScenarioAction {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "scenario_id", nullable = false)
    private Scenario scenario;  // ← поле должно называться точно так

    @Column(name = "sensor_id", nullable = false)
    private String sensorId;

    @ManyToOne
    @JoinColumn(name = "action_id", nullable = false)
    private Action action;
}