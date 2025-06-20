-- 创建顶点表：存储人物信息
CREATE TABLE hw_person (
    nickname varchar,  -- 人物昵称（字符串类型）
    p_id bigint        -- 人物唯一标识（长整型）
) WITH (
    type = 'file',  -- 数据源类型为文件
    geaflow.dsl.window.size = -1,  -- 批处理模式（-1表示全量读取）
    geaflow.dsl.file.path = 'resource:///data/grapth_hw_vertex.txt'  -- 顶点数据文件路径
);

-- 创建边表：存储好友关系
CREATE TABLE hw_friend (
    src_id bigint,    -- 源顶点ID（好友关系发起方）
    target_id bigint  -- 目标顶点ID（好友关系接收方）
) WITH (
    type = 'file',  -- 数据源类型为文件
    geaflow.dsl.window.size = -1,  -- 批处理模式
    geaflow.dsl.file.path = 'resource:///data/grapth_hw_edges.txt'  -- 边数据文件路径
);

-- 创建图结构：定义图模型
CREATE GRAPH mutualConnection (  -- 图名：mutualConnection（相互联系）
    -- 顶点定义：person类型
    Vertex person USING hw_person  -- 使用hw_person表作为顶点表
        WITH ID(p_id),            -- 指定顶点ID字段为p_id
    
    -- 边定义：friends类型
    Edge friends USING hw_friend  -- 使用hw_friend表作为边表
        WITH ID(src_id, target_id)  -- 指定边ID由(src_id, target_id)组成
) WITH (
    storeType = 'memory',  -- 图存储类型：内存存储（高性能）
    shardCount = 2         -- 图分片数：2个分片（并行处理）
);