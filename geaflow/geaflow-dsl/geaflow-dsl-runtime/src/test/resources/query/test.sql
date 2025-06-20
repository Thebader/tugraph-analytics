-- 创建结果表，用于存储算法输出
CREATE TABLE result_tb (
    perid bigint,           -- 顶点ID字段（参与环的顶点）
    circle_length bigint   -- 环长度字段（顶点参与的最小环的长度）
) WITH (
    type = 'file',         -- 指定输出类型为文件
    geaflow.dsl.file.path = '${target}'  -- 文件输出路径（运行时由变量${target}指定）
);

-- 指定使用的图：mutualConnection（预先创建的人物关系图）
USE GRAPH mutualConnection;

-- 调用图算法并将结果写入result_tb表
INSERT INTO result_tb
-- 调用单顶点环检测算法 single_vertex_circles_detection()
CALL single_vertex_circles_detection() 
-- 指定算法输出列名（必须与算法定义的输出类型匹配）
YIELD (perid, circle_length)  
-- 返回结果列（可进行额外处理，这里直接返回算法结果）
RETURN perid, circle_length;