package org.astar.thunder.scheduler;

import org.astar.thunder.dependency.NarrowDependency;
import org.astar.thunder.dependency.ShuffleDependency;
import org.astar.thunder.rdd.RDD;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class JobSchedulerTest {
  @Test
  @SuppressWarnings("unchecked")
  public void given_whenNoShuffleDependencies_thenSingleStage() {
    RDD<String> mockRdd = mock(RDD.class);
    RDD<String> mockRdd2 = mock(RDD.class);
    NarrowDependency<String> mockNarrowDependency = mock(NarrowDependency.class);

    when(mockRdd.dependencies()).thenReturn(new ArrayList<>(List.of(mockNarrowDependency)));
    when(mockNarrowDependency.rdd()).thenReturn(mockRdd2);
    when(mockRdd2.dependencies()).thenReturn(new ArrayList<>());

    ArrayList<Stage> stages = new JobScheduler().createStages(mockRdd);
    Assertions.assertEquals(1, stages.size());
    Assertions.assertEquals(0, stages.get(0).getStageId());
    Assertions.assertEquals(0, stages.get(0).getParentStageIds().size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void given_whenOneShuffleDependencies_thenTwoStage() {
    RDD<String> mockRdd = mock(RDD.class);
    ShuffleDependency<String> mockShuffleDependency = mock(ShuffleDependency.class);

    when(mockRdd.dependencies()).thenReturn(new ArrayList<>(List.of(mockShuffleDependency)));
    RDD<String> newMockRdd = mock(RDD.class);
    when(mockShuffleDependency.rdd()).thenReturn(newMockRdd);
    when(newMockRdd.dependencies()).thenReturn(new ArrayList<>());


    ArrayList<Stage> stages = new JobScheduler().createStages(mockRdd);
    Assertions.assertEquals(2, stages.size());
    Assertions.assertEquals(0, stages.get(0).getStageId());
    Assertions.assertEquals(0, stages.get(0).getParentStageIds().size());

    Assertions.assertEquals(1, stages.get(1).getStageId());
    Assertions.assertTrue(new ArrayList<>(List.of(0)).containsAll(stages.get(1).getParentStageIds()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void given_whenMultipleDependencies_thenBreakStageAtShuffleBoundary() {
    /**
     *<pre>
     *              ______ RDD1 ______
     *            /                   \
     *         RDD2                  RDD3
     *           |                     |
     *         RDD4              ___ RDD5 ___
     *                         /             \
     *                       RDD6            RDD7
     *</pre>
     */

    RDD<String> mockRdd1 = mock(RDD.class);
    RDD<String> mockRdd2 = mock(RDD.class);
    RDD<String> mockRdd3 = mock(RDD.class);
    RDD<String> mockRdd4 = mock(RDD.class);
    RDD<String> mockRdd5 = mock(RDD.class);
    RDD<String> mockRdd6 = mock(RDD.class);
    RDD<String> mockRdd7 = mock(RDD.class);
    NarrowDependency<String> mockNarrowDependency1_2 = mock(NarrowDependency.class);
    NarrowDependency<String> mockNarrowDependency1_3 = mock(NarrowDependency.class);
    NarrowDependency<String> mockNarrowDependency3_5 = mock(NarrowDependency.class);
    ShuffleDependency<String> mockShuffleDependency2_4 = mock(ShuffleDependency.class);
    ShuffleDependency<String> mockShuffleDependency5_6 = mock(ShuffleDependency.class);
    ShuffleDependency<String> mockShuffleDependency5_7 = mock(ShuffleDependency.class);
    when(mockRdd1.dependencies()).thenReturn(new ArrayList<>(List.of(mockNarrowDependency1_2, mockNarrowDependency1_3)));
    when(mockNarrowDependency1_2.rdd()).thenReturn(mockRdd2);
    when(mockNarrowDependency1_3.rdd()).thenReturn(mockRdd3);
    when(mockRdd2.dependencies()).thenReturn(new ArrayList<>(List.of(mockShuffleDependency2_4)));
    when(mockShuffleDependency2_4.rdd()).thenReturn(mockRdd4);
    when(mockRdd4.dependencies()).thenReturn(new ArrayList<>());
    when(mockRdd3.dependencies()).thenReturn(new ArrayList<>(List.of(mockNarrowDependency3_5)));
    when(mockNarrowDependency3_5.rdd()).thenReturn(mockRdd5);
    when(mockRdd5.dependencies()).thenReturn(new ArrayList<>(List.of(mockShuffleDependency5_6, mockShuffleDependency5_7)));
    when(mockShuffleDependency5_6.rdd()).thenReturn(mockRdd6);
    when(mockRdd6.dependencies()).thenReturn(new ArrayList<>());
    when(mockShuffleDependency5_7.rdd()).thenReturn(mockRdd7);
    when(mockRdd7.dependencies()).thenReturn(new ArrayList<>());

    ArrayList<Stage> stages = new JobScheduler().createStages(mockRdd1);

    Assertions.assertEquals(4, stages.size());
    Assertions.assertEquals(0, stages.get(0).getStageId());
    Assertions.assertEquals(0, stages.get(0).getParentStageIds().size());

    Assertions.assertEquals(1, stages.get(1).getStageId());
    Assertions.assertEquals(0, stages.get(0).getParentStageIds().size());

    Assertions.assertEquals(2, stages.get(2).getStageId());
    Assertions.assertEquals(0, stages.get(0).getParentStageIds().size());

    Assertions.assertEquals(3, stages.get(3).getStageId());
    Assertions.assertTrue(new ArrayList<>(List.of(0, 1, 2)).containsAll(stages.get(1).getParentStageIds()));
  }
}