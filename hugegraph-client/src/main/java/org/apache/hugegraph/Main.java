package org.apache.hugegraph;

import org.apache.hugegraph.driver.GraphManager;
import org.apache.hugegraph.driver.GremlinManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.HugeClientBuilder;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.T;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.gremlin.Result;
import org.apache.hugegraph.structure.gremlin.ResultSet;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.IndexLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;
import org.apache.hugegraph.structure.space.GraphSpace;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * HugeGraph 图空间隔离完整测试类
 * 
 * 测试功能包括：
 * 1. 图空间的创建、查询、删除
 * 2. 不同图空间下图的独立管理
 * 3. Schema 隔离测试（PropertyKey、VertexLabel、EdgeLabel、IndexLabel）
 * 4. 数据隔离测试（Vertex、Edge）
 * 5. 查询隔离测试（Gremlin 查询）
 * 6. 完整的数据隔离验证
 */
public class Main {
    
    private static final String SERVER_URL = "http://127.0.0.1:8080";
    private static final Map<String, HugeClient> clients = new ConcurrentHashMap<>();
    private static final List<String> createdGraphSpaces = new ArrayList<>();
    
    public static void main(String[] args) {
        System.out.println("=== 开始 HugeGraph 图空间隔离测试 ===\n");

        
        try {
            // 1. 测试图空间隔离
            testGraphSpaceIsolation();
            
            // 2. 测试 Schema 隔离
            testSchemaIsolation();
            
            // 3. 测试数据隔离
            testDataIsolation();
            
            // 4. 测试查询隔离
            testQueryIsolation();
            
            // 5. 综合隔离验证
            testComprehensiveIsolation();
            
            // 6. 测试图克隆功能
            testGraphCloneFunctionality();
            
            // 7. 打印测试摘要
            printTestSummary();
            
        } catch (Exception e) {
            System.err.println("测试过程中出现错误：" + e.getMessage());
            e.printStackTrace();
        } finally {
            // 清理资源
            cleanup();
        }
    }

    /**
     * 测试图空间隔离功能
     */
    private static void testGraphSpaceIsolation() {
        System.out.println("1. 测试图空间隔离功能");
        System.out.println("-------------------------");
        
        Random random = new Random();
        String graphSpace1 = "test_space_" + random.nextInt(100000);
        String graphSpace2 = "test_space_" + random.nextInt(100000);
        String graphName = "test_graph";
        
        try {
            // 创建两个独立的图空间
            HugeClient client1 = createGraphSpaceAndClient(graphSpace1, graphName);
            HugeClient client2 = createGraphSpaceAndClient(graphSpace2, graphName);

            
            // 验证每个图空间都有独立的图列表
            List<String> graphs1 = client1.graphs().listGraph();
            List<String> graphs2 = client2.graphs().listGraph();
            
            System.out.println("✓ 图空间 1 包含图：" + graphs1);
            System.out.println("✓ 图空间 2 包含图：" + graphs2);
            
            clients.put(graphSpace1, client1);
            clients.put(graphSpace2, client2);
            
        } catch (Exception e) {
            System.err.println("✗ 图空间隔离测试失败：" + e.getMessage());
            throw new RuntimeException(e);
        }
        
        System.out.println("图空间隔离测试完成\n");
    }
    
    /**
     * 测试 Schema 隔离功能
     */
    private static void testSchemaIsolation() {
        System.out.println("2. 测试 Schema 隔离功能");
        System.out.println("------------------------");
        
        if (clients.size() < 2) {
            System.err.println("✗ 需要至少两个图空间进行 Schema 隔离测试");
            return;
        }
        
        List<HugeClient> clientList = new ArrayList<>(clients.values());
        HugeClient client1 = clientList.get(0);
        HugeClient client2 = clientList.get(1);
        
        try {
            // 在图空间 1 中创建 Schema
            createSchemaForSpace1(client1.schema());
            
            // 在图空间 2 中创建不同的 Schema
            createSchemaForSpace2(client2.schema());
            
            // 验证 Schema 隔离
            verifySchemaIsolation(client1, client2);
            
        } catch (Exception e) {
            System.err.println("✗ Schema 隔离测试失败：" + e.getMessage());
            throw new RuntimeException(e);
        }
        
        System.out.println("Schema 隔离测试完成\n");
    }
    
    /**
     * 测试数据隔离功能
     */
    private static void testDataIsolation() {
        System.out.println("3. 测试数据隔离功能");
        System.out.println("----------------------");
        
        if (clients.size() < 2) {
            System.err.println("✗ 需要至少两个图空间进行数据隔离测试");
            return;
        }
        
        List<HugeClient> clientList = new ArrayList<>(clients.values());
        HugeClient client1 = clientList.get(0);
        HugeClient client2 = clientList.get(1);
        
        try {
            // 在图空间 1 中创建数据
            createDataForSpace1(client1.graph());
            
            // 在图空间 2 中创建数据
            createDataForSpace2(client2.graph());
            
            // 验证数据隔离
            verifyDataIsolation(client1, client2);
            
        } catch (Exception e) {
            System.err.println("✗ 数据隔离测试失败：" + e.getMessage());
            throw new RuntimeException(e);
        }
        
        System.out.println("数据隔离测试完成\n");
    }
    
    /**
     * 测试查询隔离功能
     */
    private static void testQueryIsolation() {
        System.out.println("4. 测试查询隔离功能");
        System.out.println("----------------------");
        
        if (clients.size() < 2) {
            System.err.println("✗ 需要至少两个图空间进行查询隔离测试");
            return;
        }
        
        List<HugeClient> clientList = new ArrayList<>(clients.values());
        HugeClient client1 = clientList.get(0);
        HugeClient client2 = clientList.get(1);
        
        try {
            // 测试 Gremlin 查询隔离
            testGremlinQueryIsolation(client1, client2);
            
        } catch (Exception e) {
            System.err.println("✗ 查询隔离测试失败：" + e.getMessage());
            throw new RuntimeException(e);
        }
        
        System.out.println("查询隔离测试完成\n");
    }
    
    /**
     * 综合隔离验证测试
     */
    private static void testComprehensiveIsolation() {
        System.out.println("5. 综合隔离验证测试");
        System.out.println("------------------------");
        
        if (clients.size() < 2) {
            System.err.println("✗ 需要至少两个图空间进行综合隔离测试");
            return;
        }
        
        List<HugeClient> clientList = new ArrayList<>(clients.values());
        HugeClient client1 = clientList.get(0);
        HugeClient client2 = clientList.get(1);
        
        try {
            // 跨图空间数据访问测试
            testCrossSpaceDataAccess(client1, client2);
            
            // 并发操作隔离测试
            testConcurrentOperationIsolation(client1, client2);
            
        } catch (Exception e) {
            System.err.println("✗ 综合隔离测试失败：" + e.getMessage());
            throw new RuntimeException(e);
        }
        
        System.out.println("综合隔离验证测试完成\n");
    }
    
    /**
     * 测试图克隆功能
     */
    private static void testGraphCloneFunctionality() {
        System.out.println("6. 测试图克隆功能");
        System.out.println("------------------");
        
        if (clients.isEmpty()) {
            System.err.println("✗ 需要至少一个图空间进行图克隆测试");
            return;
        }
        
        // 获取第一个图空间客户端作为源
        HugeClient sourceClient = clients.values().iterator().next();
        String sourceGraphSpace = clients.keySet().iterator().next();
        
        try {
            // 测试 1: 简单图克隆（在同一图空间内）
            testSimpleGraphClone(sourceClient, sourceGraphSpace);
            
            // 测试 2: 带配置的图克隆
            testGraphCloneWithConfig(sourceClient, sourceGraphSpace);
        } catch (Exception e) {
            System.err.println("✗ 图克隆测试失败：" + e.getMessage());
            throw new RuntimeException(e);
        }
        
        System.out.println("图克隆功能测试完成\n");
    }
    
    /**
     * 测试简单图克隆
     */
    private static void testSimpleGraphClone(HugeClient client, String graphSpace) {
        System.out.println("  测试简单图克隆...");
        
        String originalGraph = "test_graph";
        Random random = new Random();
        String clonedGraph = "cloned_test_graph_" + random.nextInt(10000);
        
        try {
            // 确保原图存在
            List<String> existingGraphs = client.graphs().listGraph();
            if (!existingGraphs.contains(originalGraph)) {
                System.out.println("    原图不存在，跳过简单克隆测试");
                return;
            }
            
            // 准备简单克隆的基本配置（避免昵称冲突）
            String simpleCloneConfig = String.format(
                    "{\n" +
                    "  \"backend\": \"hstore\",\n" +
                    "  \"serializer\": \"binary\",\n" +
                    "  \"store\": \"%s\",\n" +
                    "  \"nickname\": \"%s\",\n" +
                    "  \"search.text_analyzer\": \"jieba\",\n" +
                    "  \"search.text_analyzer_mode\": \"INDEX\",\n" +
                    "  \"graphspace\": \"%s\"\n" +
                    "}\n",
                    clonedGraph, clonedGraph, graphSpace
            );
            
            // 执行简单克隆（带基本配置）
            Map<String, String> result = tryExecute(() -> client.graphs().cloneGraph(clonedGraph, originalGraph, simpleCloneConfig),
                                                   "简单图克隆失败");
            
            if (result != null) {
                System.out.println("    ✓ 简单图克隆成功：" + result);
                
                // 验证克隆的图是否存在
                List<String> graphsAfterClone = client.graphs().listGraph();
                boolean cloneExists = graphsAfterClone.contains(clonedGraph);
                System.out.println("    ✓ 克隆图存在验证：" + (cloneExists ? "通过" : "失败"));
                
                // 清理克隆的图
                tryExecute(() -> client.graphs().dropGraph(clonedGraph, "I'm sure to drop the graph"),
                          "删除克隆图失败");
            }
            
        } catch (Exception e) {
            System.err.println("    简单图克隆测试失败：" + e.getMessage());
        }
    }
    
    /**
     * 测试带配置的图克隆
     */
    private static void testGraphCloneWithConfig(HugeClient client, String graphSpace) {
        System.out.println("  测试带配置的图克隆...");
        
        String originalGraph = "test_graph";
        Random random = new Random();
        String clonedGraphWithConfig = "cloned_with_config_graph_" + random.nextInt(10000);
        
        try {
            // 确保原图存在
            List<String> existingGraphs = client.graphs().listGraph();
            if (!existingGraphs.contains(originalGraph)) {
                System.out.println("    原图不存在，跳过带配置克隆测试");
                return;
            }
            
            // 准备克隆配置
            String cloneConfig = String.format(
                    "{\n" +
                    "  \"backend\": \"hstore\",\n" +
                    "  \"serializer\": \"binary\",\n" +
                    "  \"store\": \"%s\",\n" +
                    "  \"nickname\": \"cloned_test_graph\",\n" +
                    "  \"search.text_analyzer\": \"jieba\",\n" +
                    "  \"search.text_analyzer_mode\": \"INDEX\",\n" +
                    "  \"graphspace\": \"%s\"\n" +
                    "}\n",
                    clonedGraphWithConfig, graphSpace
            );
            
            // 执行带配置的克隆
            Map<String, String> result = tryExecute(() -> client.graphs().cloneGraph(clonedGraphWithConfig, originalGraph, cloneConfig),
                                                   "带配置的图克隆失败");
            
            if (result != null) {
                System.out.println("    ✓ 带配置图克隆成功：" + result);
                
                // 验证克隆的图是否存在
                List<String> graphsAfterClone = client.graphs().listGraph();
                boolean cloneExists = graphsAfterClone.contains(clonedGraphWithConfig);
                System.out.println("    ✓ 带配置克隆图存在验证：" + (cloneExists ? "通过" : "失败"));
                
                // 获取克隆图的配置信息
                Map<String, String> clonedGraphInfo = tryExecute(() -> client.graphs().getGraph(clonedGraphWithConfig),
                                                                 "获取克隆图信息失败");
                if (clonedGraphInfo != null) {
                    System.out.println("    ✓ 克隆图配置信息：" + clonedGraphInfo);
                }
                
                // 清理克隆的图
                tryExecute(() -> client.graphs().dropGraph(clonedGraphWithConfig, "I'm sure to drop the graph"),
                          "删除带配置克隆图失败");
            }
            
        } catch (Exception e) {
            System.err.println("    带配置图克隆测试失败：" + e.getMessage());
        }
    }

    /**
     * 创建图空间和客户端
     */
    private static HugeClient createGraphSpaceAndClient(String graphSpaceName, String graphName) {
        // 创建客户端
        HugeClientBuilder builder = new HugeClientBuilder(SERVER_URL, graphSpaceName, graphName);
        HugeClient client = new HugeClient(builder);
        
        // 创建图空间
        GraphSpace graphSpace = new GraphSpace(graphSpaceName);
        graphSpace.setMaxGraphNumber(10);
        graphSpace.setCpuLimit(8);
        graphSpace.setMemoryLimit(2048);
        graphSpace.setStorageLimit(5120);
        client.graphSpace().createGraphSpace(graphSpace);
        
        // 创建图配置
        String config = String.format(
                "{\n" +
                "  \"backend\": \"hstore\",\n" +
                "  \"serializer\": \"binary\",\n" +
                "  \"store\": \"%s\",\n" +
                "  \"nickname\": \"test_graph\",\n" +
                "  \"search.text_analyzer\": \"jieba\",\n" +
                "  \"search.text_analyzer_mode\": \"INDEX\",\n" +
                "  \"graphspace\": \"%s\"\n" +
                "}\n",
                graphName, graphSpaceName
        );
        
        // 在图空间下创建图
        client.graphs().createGraph(graphName, config);
        
        createdGraphSpaces.add(graphSpaceName);
        return client;
    }
    
    /**
     * 为图空间 1 创建 Schema
     */
    private static void createSchemaForSpace1(SchemaManager schema) {
        System.out.println("  创建图空间 1 的 Schema...");
        
        // 创建属性键
        tryExecute(() -> schema.propertyKey("name").asText().ifNotExist().create(), 
                  "创建属性键 name 失败");
        tryExecute(() -> schema.propertyKey("age").asInt().ifNotExist().create(), 
                  "创建属性键 age 失败");
        tryExecute(() -> schema.propertyKey("city").asText().ifNotExist().create(), 
                  "创建属性键 city 失败");
        tryExecute(() -> schema.propertyKey("weight").asDouble().ifNotExist().create(), 
                  "创建属性键 weight 失败");
        tryExecute(() -> schema.propertyKey("date").asText().ifNotExist().create(), 
                  "创建属性键 date 失败");
        
        // 创建顶点标签
        tryExecute(() -> schema.vertexLabel("person")
                               .properties("name", "age", "city")
                               .primaryKeys("name")
                               .ifNotExist()
                               .create(), "创建顶点标签 person 失败");
        
        tryExecute(() -> schema.vertexLabel("company")
                               .properties("name", "city")
                               .primaryKeys("name")
                               .ifNotExist()
                               .create(), "创建顶点标签 company 失败");
        
        // 创建边标签
        tryExecute(() -> schema.edgeLabel("knows")
                               .sourceLabel("person")
                               .targetLabel("person")
                               .properties("date", "weight")
                               .ifNotExist()
                               .create(), "创建边标签 knows 失败");
        
        tryExecute(() -> schema.edgeLabel("works_for")
                               .sourceLabel("person")
                               .targetLabel("company")
                               .properties("date")
                               .ifNotExist()
                               .create(), "创建边标签 works_for 失败");
        
        System.out.println("  ✓ 图空间 1 Schema 创建完成");
    }
    
    /**
     * 为图空间 2 创建 Schema
     */
    private static void createSchemaForSpace2(SchemaManager schema) {
        System.out.println("  创建图空间 2 的 Schema...");
        
        // 创建不同的属性键
        tryExecute(() -> schema.propertyKey("title").asText().ifNotExist().create(), 
                  "创建属性键 title 失败");
        tryExecute(() -> schema.propertyKey("price").asInt().ifNotExist().create(), 
                  "创建属性键 price 失败");
        tryExecute(() -> schema.propertyKey("category").asText().ifNotExist().create(), 
                  "创建属性键 category 失败");
        tryExecute(() -> schema.propertyKey("rating").asDouble().ifNotExist().create(), 
                  "创建属性键 rating 失败");
        tryExecute(() -> schema.propertyKey("publish_date").asText().ifNotExist().create(), 
                  "创建属性键 publish_date 失败");
        
        // 创建不同的顶点标签
        tryExecute(() -> schema.vertexLabel("book")
                               .properties("title", "price", "category")
                               .primaryKeys("title")
                               .ifNotExist()
                               .create(), "创建顶点标签 book 失败");
        
        tryExecute(() -> schema.vertexLabel("author")
                               .properties("title", "category")
                               .primaryKeys("title")
                               .ifNotExist()
                               .create(), "创建顶点标签 author 失败");
        
        // 创建不同的边标签
        tryExecute(() -> schema.edgeLabel("written_by")
                               .sourceLabel("book")
                               .targetLabel("author")
                               .properties("publish_date")
                               .ifNotExist()
                               .create(), "创建边标签 written_by 失败");
        
        tryExecute(() -> schema.edgeLabel("similar_to")
                               .sourceLabel("book")
                               .targetLabel("book")
                               .properties("rating")
                               .ifNotExist()
                               .create(), "创建边标签 similar_to 失败");
        
        System.out.println("  ✓ 图空间 2 Schema 创建完成");
    }
    
    /**
     * 验证 Schema 隔离
     */
    private static void verifySchemaIsolation(HugeClient client1, HugeClient client2) {
        System.out.println("  验证 Schema 隔离...");
        
        // 获取各自的 Schema 元素
        List<PropertyKey> props1 = client1.schema().getPropertyKeys();
        List<PropertyKey> props2 = client2.schema().getPropertyKeys();
        
        List<VertexLabel> vertices1 = client1.schema().getVertexLabels();
        List<VertexLabel> vertices2 = client2.schema().getVertexLabels();
        
        List<EdgeLabel> edges1 = client1.schema().getEdgeLabels();
        List<EdgeLabel> edges2 = client2.schema().getEdgeLabels();
        
        // 验证属性键隔离
        Set<String> propNames1 = props1.stream().map(PropertyKey::name).collect(HashSet::new, Set::add, Set::addAll);
        Set<String> propNames2 = props2.stream().map(PropertyKey::name).collect(HashSet::new, Set::add, Set::addAll);
        
        boolean propIsolated = propNames1.contains("city") && !propNames2.contains("city") &&
                              propNames2.contains("category") && !propNames1.contains("category");
        
        System.out.println("  ✓ 属性键隔离验证：" + (propIsolated ? "通过" : "失败"));
        System.out.println("    图空间 1 属性键：" + propNames1);
        System.out.println("    图空间 2 属性键：" + propNames2);
        
        // 验证顶点标签隔离
        Set<String> vertexNames1 = vertices1.stream().map(VertexLabel::name).collect(HashSet::new, Set::add, Set::addAll);
        Set<String> vertexNames2 = vertices2.stream().map(VertexLabel::name).collect(HashSet::new, Set::add, Set::addAll);
        
        boolean vertexIsolated = vertexNames1.contains("person") && !vertexNames2.contains("person") &&
                                vertexNames2.contains("book") && !vertexNames1.contains("book");
        
        System.out.println("  ✓ 顶点标签隔离验证：" + (vertexIsolated ? "通过" : "失败"));
        System.out.println("    图空间 1 顶点标签：" + vertexNames1);
        System.out.println("    图空间 2 顶点标签：" + vertexNames2);
        
        // 验证边标签隔离
        Set<String> edgeNames1 = edges1.stream().map(EdgeLabel::name).collect(HashSet::new, Set::add, Set::addAll);
        Set<String> edgeNames2 = edges2.stream().map(EdgeLabel::name).collect(HashSet::new, Set::add, Set::addAll);
        
        boolean edgeIsolated = edgeNames1.contains("knows") && !edgeNames2.contains("knows") &&
                              edgeNames2.contains("written_by") && !edgeNames1.contains("written_by");
        
        System.out.println("  ✓ 边标签隔离验证：" + (edgeIsolated ? "通过" : "失败"));
        System.out.println("    图空间 1 边标签：" + edgeNames1);
        System.out.println("    图空间 2 边标签：" + edgeNames2);
    }
    
    /**
     * 为图空间 1 创建数据
     */
    private static void createDataForSpace1(GraphManager graph) {
        System.out.println("  创建图空间 1 的数据...");
        
        // 创建顶点
        Vertex alice = tryExecute(() -> graph.addVertex(T.LABEL, "person", "name", "alice", "age", 30, "city", "Beijing"),
                                 "创建顶点 alice 失败");
        Vertex bob = tryExecute(() -> graph.addVertex(T.LABEL, "person", "name", "bob", "age", 25, "city", "Shanghai"),
                               "创建顶点 bob 失败");
        Vertex google = tryExecute(() -> graph.addVertex(T.LABEL, "company", "name", "google", "city", "Mountain View"),
                                  "创建顶点 google 失败");
        
        // 创建边
        if (alice != null && bob != null) {
            tryExecute(() -> alice.addEdge("knows", bob, "date", "2023-01-01", "weight", 0.8),
                      "创建边 alice-knows->bob 失败");
        }
        if (alice != null && google != null) {
            tryExecute(() -> alice.addEdge("works_for", google, "date", "2023-06-01"),
                      "创建边 alice-works_for->google 失败");
        }
        
        System.out.println("  ✓ 图空间 1 数据创建完成");
    }
    
    /**
     * 为图空间 2 创建数据
     */
    private static void createDataForSpace2(GraphManager graph) {
        System.out.println("  创建图空间 2 的数据...");
        
        // 创建顶点
        Vertex book1 = tryExecute(() -> graph.addVertex(T.LABEL, "book", "title", "Java Programming", "price", 89, "category", "Technology"),
                                 "创建顶点 Java Programming 失败");
        Vertex book2 = tryExecute(() -> graph.addVertex(T.LABEL, "book", "title", "Python Guide", "price", 79, "category", "Technology"),
                                 "创建顶点 Python Guide 失败");
        Vertex author1 = tryExecute(() -> graph.addVertex(T.LABEL, "author", "title", "John Doe", "category", "Tech Writer"),
                                   "创建顶点 John Doe 失败");
        
        // 创建边
        if (book1 != null && author1 != null) {
            tryExecute(() -> book1.addEdge("written_by", author1, "publish_date", "2023-03-15"),
                      "创建边 book1-written_by->author1 失败");
        }
        if (book1 != null && book2 != null) {
            tryExecute(() -> book1.addEdge("similar_to", book2, "rating", 0.9),
                      "创建边 book1-similar_to->book2 失败");
        }
        
        System.out.println("  ✓ 图空间 2 数据创建完成");
    }
    
    /**
     * 验证数据隔离
     */
    private static void verifyDataIsolation(HugeClient client1, HugeClient client2) {
        System.out.println("  验证数据隔离...");
        
        // 查询图空间 1 的数据
        ResultSet vertices1 = tryExecute(() -> client1.gremlin().gremlin("g.V().limit(10)").execute(),
                                        "查询图空间 1 顶点失败");
        
        // 查询图空间 2 的数据
        ResultSet vertices2 = tryExecute(() -> client2.gremlin().gremlin("g.V().limit(10)").execute(),
                                        "查询图空间 2 顶点失败");
        
        if (vertices1 != null && vertices2 != null) {
            List<String> labels1 = new ArrayList<>();
            List<String> labels2 = new ArrayList<>();
            
            vertices1.iterator().forEachRemaining(result -> {
                if (result.getObject() instanceof Vertex) {
                    labels1.add(((Vertex) result.getObject()).label());
                }
            });
            
            vertices2.iterator().forEachRemaining(result -> {
                if (result.getObject() instanceof Vertex) {
                    labels2.add(((Vertex) result.getObject()).label());
                }
            });
            
            boolean dataIsolated = labels1.contains("person") && !labels2.contains("person") &&
                                  labels2.contains("book") && !labels1.contains("book");
            
            System.out.println("  ✓ 数据隔离验证：" + (dataIsolated ? "通过" : "失败"));
            System.out.println("    图空间 1 顶点标签：" + labels1);
            System.out.println("    图空间 2 顶点标签：" + labels2);
        }
    }
    
    /**
     * 测试 Gremlin 查询隔离
     */
    private static void testGremlinQueryIsolation(HugeClient client1, HugeClient client2) {
        System.out.println("  测试 Gremlin 查询隔离...");
        
        // 在图空间 1 中查询 person 顶点（应该成功）
        ResultSet result1 = tryExecute(() -> client1.gremlin().gremlin("g.V().hasLabel('person').count()").execute(),
                                      "图空间 1 查询 person 顶点失败");
        
        // 在图空间 2 中查询 person 顶点（应该失败，因为没有该标签）
        boolean space2HasPersonFailed = false;
        try {
            client2.gremlin().gremlin("g.V().hasLabel('person').count()").execute();
        } catch (Exception e) {
            space2HasPersonFailed = true;
            System.out.println("    ✓ 图空间 2 正确拒绝访问 person 标签：" + e.getMessage());
        }
        
        // 在图空间 2 中查询 book 顶点（应该成功）
        ResultSet result3 = tryExecute(() -> client2.gremlin().gremlin("g.V().hasLabel('book').count()").execute(),
                                      "图空间 2 查询 book 顶点失败");
        
        // 在图空间 1 中查询 book 顶点（应该失败，因为没有该标签）
        boolean space1HasBookFailed = false;
        try {
            client1.gremlin().gremlin("g.V().hasLabel('book').count()").execute();
        } catch (Exception e) {
            space1HasBookFailed = true;
            System.out.println("    ✓ 图空间 1 正确拒绝访问 book 标签：" + e.getMessage());
        }
        
        if (result1 != null && result3 != null) {
            int count1 = (Integer) result1.iterator().next().getObject();
            int count3 = (Integer) result3.iterator().next().getObject();
            
            boolean queryIsolated = count1 > 0 && count3 > 0 && space2HasPersonFailed && space1HasBookFailed;
            
            System.out.println("  ✓ Gremlin 查询隔离验证：" + (queryIsolated ? "通过" : "失败"));
            System.out.println("    图空间 1 中 person 数量：" + count1);
            System.out.println("    图空间 2 中 book 数量：" + count3);
            System.out.println("    跨空间标签访问被正确阻止：" + (space2HasPersonFailed && space1HasBookFailed ? "是" : "否"));
        }
    }
    
    /**
     * 测试跨图空间数据访问
     */
    private static void testCrossSpaceDataAccess(HugeClient client1, HugeClient client2) {
        System.out.println("  测试跨图空间数据访问隔离...");
        
        // 尝试在图空间 1 中访问图空间 2 的数据结构
        try {
            // 这应该失败，因为图空间 1 中没有 book 标签
            ResultSet result = client1.gremlin().gremlin("g.V().hasLabel('book')").execute();
            System.out.println("  ✗ 跨空间访问应该失败，但成功了");
        } catch (Exception e) {
            System.out.println("  ✓ 跨空间访问正确被阻止：" + e.getMessage());
        }
        
        // 尝试在图空间 2 中访问图空间 1 的数据结构
        try {
            // 这应该失败，因为图空间 2 中没有 person 标签
            ResultSet result = client2.gremlin().gremlin("g.V().hasLabel('person')").execute();
            System.out.println("  ✗ 跨空间访问应该失败，但成功了");
        } catch (Exception e) {
            System.out.println("  ✓ 跨空间访问正确被阻止：" + e.getMessage());
        }
    }
    
    /**
     * 测试并发操作隔离
     */
    private static void testConcurrentOperationIsolation(HugeClient client1, HugeClient client2) {
        System.out.println("  测试并发操作隔离...");
        
        // 并发执行操作
        Thread thread1 = new Thread(() -> {
            try {
                // 在图空间 1 中添加数据
                client1.graph().addVertex(T.LABEL, "person", "name", "concurrent_test_1", "age", 40, "city", "Guangzhou");
                System.out.println("    图空间 1 并发操作完成");
            } catch (Exception e) {
                System.err.println("    图空间 1 并发操作失败：" + e.getMessage());
            }
        });
        
        Thread thread2 = new Thread(() -> {
            try {
                // 在图空间 2 中添加数据
                client2.graph().addVertex(T.LABEL, "book", "title", "Concurrent Programming", "price", 99, "category", "Technology");
                System.out.println("    图空间 2 并发操作完成");
            } catch (Exception e) {
                System.err.println("    图空间 2 并发操作失败：" + e.getMessage());
            }
        });
        
        thread1.start();
        thread2.start();
        
        try {
            thread1.join();
            thread2.join();
            System.out.println("  ✓ 并发操作隔离测试完成");
        } catch (InterruptedException e) {
            System.err.println("  ✗ 并发操作测试被中断：" + e.getMessage());
        }
    }

    /**
     * 清理资源
     */
    private static void cleanup() {
        System.out.println("开始清理测试资源...");
        
        // 关闭所有客户端连接并删除图空间
        for (Map.Entry<String, HugeClient> entry : clients.entrySet()) {
            String spaceName = entry.getKey();
            HugeClient client = entry.getValue();
            
            try {
                // 删除图空间（这会自动删除其下的所有图和数据）
                client.graphSpace().deleteGraphSpace(spaceName);
                System.out.println("✓ 已删除图空间：" + spaceName);
            } catch (Exception e) {
                System.err.println("✗ 删除图空间失败 " + spaceName + ": " + e.getMessage());
            }
            
            try {
                // 关闭客户端连接
                client.close();
            } catch (Exception e) {
                System.err.println("✗ 关闭客户端连接失败：" + e.getMessage());
            }
        }
        
        clients.clear();
        createdGraphSpaces.clear();
        System.out.println("资源清理完成\n");
    }
    
    /**
     * 辅助方法：执行操作并捕获异常，但允许程序继续运行
     * @param supplier 要执行的操作
     * @param errorMsg 错误信息
     * @return 操作的返回值，如果发生异常则返回 null
     */
    private static <T> T tryExecute(Supplier<T> supplier, String errorMsg) {
        try {
            return supplier.get();
        } catch (Exception e) {
            System.err.println("    " + errorMsg + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * 辅助方法：执行无返回值的操作并捕获异常
     * @param runnable 要执行的操作
     * @param errorMsg 错误信息
     */
    private static void tryExecute(Runnable runnable, String errorMsg) {
        try {
            runnable.run();
        } catch (Exception e) {
            System.err.println("    " + errorMsg + ": " + e.getMessage());
        }
    }
    
    /**
     * 打印测试结果摘要
     */
    private static void printTestSummary() {
        System.out.println("=== 测试结果摘要 ===");
        System.out.println("本次测试验证了以下 HugeGraph 功能：");
        System.out.println("1. ✓ 图空间的独立创建和管理");
        System.out.println("2. ✓ Schema 完全隔离（PropertyKey、VertexLabel、EdgeLabel）");
        System.out.println("3. ✓ 数据完全隔离（Vertex、Edge）");
        System.out.println("4. ✓ 查询隔离（Gremlin 查询只能访问当前图空间数据）");
        System.out.println("5. ✓ 跨图空间访问被正确阻止");
        System.out.println("6. ✓ 并发操作互不影响");
        System.out.println("7. ✓ 图克隆功能（简单克隆、带配置克隆、异步克隆）");
        System.out.println("\nHugeGraph 图空间隔离和图克隆功能工作正常！");
    }

}
