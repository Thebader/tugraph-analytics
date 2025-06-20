package com.antgroup.geaflow.dsl.udf.graph;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;
import java.util.Optional;

@Description(name = "single_vertex_circles_detection", description = "detect self-loop or circle for each vertex by message passing")
public class SingleVertexCirclesDetection implements AlgorithmUserFunction<Long, Tuple<Long, Integer>> {
    private AlgorithmRuntimeContext<Long, Tuple<Long, Integer>> context;
    // 最大深度
    private static final int MAX_DEPTH = 10; 
    //保存环路上的点 用于最终输出
    private Set<Long> verticesInCircle = new HashSet<>(); 
    private Set<Tuple<Long, Integer>> circleResults = new HashSet<>();

    @Override
    public void init(AlgorithmRuntimeContext<Long, Tuple<Long, Integer>> context, 
Object[] params) {
        this.context = context;
    }

    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Long, Integer>> messages) {
        Long selfId = (Long) TypeCastUtil.cast(vertex.getId(), Long.class);
        long iteration = context.getCurrentIterationId();
        if (iteration == 1L) {

            // 首轮需要每个顶点向所有出边邻居发送Tuple
            Tuple<Long, Integer> msg = Tuple.of(selfId, 1);
            for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                Long targetId = (Long) TypeCastUtil.cast(edge.getTargetId(), 
Long.class);
                context.sendMessage(targetId, msg);
            }
        } else {
            // 之后的轮次
            // 判断是否成环，否则继续转发
            while (messages.hasNext()) {
                Tuple<Long, Integer> msg = messages.next();
                Long startId = msg.getF0();
                int pathLen = msg.getF1();
                if (Objects.equals(selfId, startId)) {
                    // 检测到环路，记录该顶点ID和环的长度到结果变量中
                    verticesInCircle.add(selfId);
                    circleResults.add(Tuple.of(selfId, pathLen));
                    continue;
                }
                if (pathLen >= MAX_DEPTH) continue;
                Tuple<Long, Integer> newMsg = Tuple.of(startId, pathLen + 1);
                for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                    Long targetId = (Long) TypeCastUtil.cast(edge.getTargetId(), 
Long.class);
                    context.sendMessage(targetId, newMsg);
                }
            }
        }
    }

    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        Long vertexId = (Long) TypeCastUtil.cast(vertex.getId(), Long.class);
        // 只输出在环中的顶点
        if (verticesInCircle.contains(vertexId)) {
            // 找到该顶点参与的最小的环
            int minCircleLength = Integer.MAX_VALUE;
            for (Tuple<Long, Integer> result : circleResults) {
                if (Objects.equals(result.getF0(), vertexId)) {
                    minCircleLength = Math.min(minCircleLength, result.getF1());
                }
            }
            if (minCircleLength != Integer.MAX_VALUE) {
                context.take(ObjectRow.create(vertexId, minCircleLength));
            }
        }
    }

    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vertex_id", graphSchema.getIdType(), false),
            new TableField("circle_length", LongType.INSTANCE, false)
        );
    }

}