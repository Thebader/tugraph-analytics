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

// 算法描述：检测单个顶点参与的自环或环（通过消息传递）
@Description(name = "single_vertex_circles_detection", 
             description = "detect self-loop or circle for each vertex by message passing")
public class SingleVertexCirclesDetection implements AlgorithmUserFunction<Long, Tuple<Long, Integer>> {
    
    private AlgorithmRuntimeContext<Long, Tuple<Long, Integer>> context; // 算法运行时上下文
    
    // 常量配置
    private static final int MAX_DEPTH = 10;  // 最大搜索深度（防止无限循环）
    
    // 状态记录
    private Set<Long> verticesInCircle = new HashSet<>();      // 记录在环中的顶点ID
    private Set<Tuple<Long, Integer>> circleResults = new HashSet<>(); // 记录环检测结果（顶点ID，环长度）

    // 初始化方法（由框架调用）
    @Override
    public void init(AlgorithmRuntimeContext<Long, Tuple<Long, Integer>> context, Object[] params) {
        this.context = context;  // 保存运行时上下文
    }

    // 顶点计算逻辑（每个迭代调用）
    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Long, Integer>> messages) {
        Long selfId = (Long) TypeCastUtil.cast(vertex.getId(), Long.class);  // 获取当前顶点ID
        long iteration = context.getCurrentIterationId();  // 获取当前迭代轮次

        // 第一轮迭代：初始化消息发送
        if (iteration == 1L) {
            // 创建初始消息：(起始顶点ID, 当前路径长度=1)
            Tuple<Long, Integer> msg = Tuple.of(selfId, 1);
            
            // 遍历所有出边，向邻居发送消息
            for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                Long targetId = (Long) TypeCastUtil.cast(edge.getTargetId(), Long.class);
                context.sendMessage(targetId, msg);  // 发送消息到目标顶点
            }
        } 
        // 后续迭代：处理接收到的消息
        else {
            while (messages.hasNext()) {
                Tuple<Long, Integer> msg = messages.next();
                Long startId = msg.getF0();   // 消息中的起始顶点ID
                int pathLen = msg.getF1();    // 当前路径长度
                
                // 检测到环：当前顶点是环的起点
                if (Objects.equals(selfId, startId)) {
                    verticesInCircle.add(selfId);          // 标记当前顶点在环中
                    circleResults.add(Tuple.of(selfId, pathLen)); // 记录环信息(顶点ID, 环长度)
                    continue;  // 不再转发此消息
                }
                
                // 深度超过阈值：停止传播
                if (pathLen >= MAX_DEPTH) continue;
                
                // 创建新消息：路径长度+1
                Tuple<Long, Integer> newMsg = Tuple.of(startId, pathLen + 1);
                
                // 转发消息给所有出边邻居
                for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                    Long targetId = (Long) TypeCastUtil.cast(edge.getTargetId(), Long.class);
                    context.sendMessage(targetId, newMsg);
                }
            }
        }
    }

    // 算法收尾处理（所有迭代结束后调用）
    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        Long vertexId = (Long) TypeCastUtil.cast(vertex.getId(), Long.class);
        
        // 只处理在环中的顶点
        if (verticesInCircle.contains(vertexId)) {
            int minCircleLength = Integer.MAX_VALUE;
            
            // 查找该顶点参与的最小环长度
            for (Tuple<Long, Integer> result : circleResults) {
                if (Objects.equals(result.getF0(), vertexId)) {
                    minCircleLength = Math.min(minCircleLength, result.getF1());
                }
            }
            
            // 输出结果：(顶点ID, 最小环长度)
            if (minCircleLength != Integer.MAX_VALUE) {
                context.take(ObjectRow.create(vertexId, minCircleLength));
            }
        }
    }

    // 定义输出数据结构
    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vertex_id", graphSchema.getIdType(), false),  // 顶点ID字段
            new TableField("circle_length", LongType.INSTANCE, false)    // 环长度字段
        );
    }
}