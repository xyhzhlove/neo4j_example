import os

from py2neo import Graph, Node, Relationship,NodeMatcher, RelationshipMatcher
from json2triple import json2triple, read_json, read_csv, read_excel, read_excel3, read_excel4, read_excel2


class EducationGraph:
    def __init__(self,host,port,username,password):
        # 连接数据库的示例
        self.neo4j_connection(host,port,username,password)

     
    def neo4j_connection(self,host,port,username,password):
         self.driver = Graph(
            host=host,  # 127.0.0.1",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
            port=port,  # neo4j 服务器监听的端口号
            user=username,  # "lhy",  # 数据库user name，如果没有更改过，应该是neo4j
            password=password)  # "lhy123")
         
    # name为节点查询关键字,查询时以节点标签Type与节点name节点标签为唯一查询条件   
    # 建立一个节点
    def create_node(self,label, attrs):
        n = "_.name=" + "\"" + attrs["name"] + "\""
        matcher = NodeMatcher(self.driver)
        # 查询是否已经存在，若存在则返回节点，否则返回None
        value = matcher.match(label).where(n).first()
        # 如果要创建的节点不存在则创建
        if value is None:
            node = Node(label, **attrs)
            n = self.driver.create(node)
            # 返回节点
            return node
        else:
            # 更新节点
            # value.update(attrs)  # 修改结点的属性
            # self.driver.push(value)  # 更新结点，注意：如果没有这一步，则结点不会被更新
            # 返回已经存在的节点
            return value
        
   # 建立两个节点之间的关系
#    创建关系通过标签、与属性
#    self只在实例方法中可使用，充当类的实例，接收的第一个参数仍是使用者传递的参数
    def create_relationship(self, label1, attrs1, label2, attrs2, r_name):
       
        value1 = self.match_node(self.driver, label1, attrs1)
        value2 = self.match_node(self.driver, label2, attrs2)
        if value1 is None or value2 is None:
            return False
        r = Relationship(value1, r_name, value2)
        self.driver.create(r)
    
    # 创建关系通过节点的方式
    def create_relationship_by_node(self, node1,rel,node2):
       if self.has_relationship(node1,node2,rel):
           return 
       ab = Relationship(node1, rel, node2)
       self.driver.create(ab)

    
    # 查询是否存在某个节点
    def match_node(self, label, attrs):
        n = "_.name=" + "\"" + attrs["name"] + "\""
        matcher = NodeMatcher(self.driver)
        return matcher.match(label).where(n).first()
    
    # 更新节点
    def update_node(self,label,attrs,new_attrs):
        node = self.match_node(label, attrs)  # 找到对应的结点
        node.update(new_attrs)  # 修改结点的属性
        self.driver.push(node)  # 更新结点，注意：如果没有这一步，则结点不会被更新

       

    # 返回节点
    def search_nodes(self,label,attrs):
        macher1 = NodeMatcher(self.driver)
        node = macher1.match(label, **attrs)  
        return node
      
    
    # 返回关系
    def search_relationships(self,r_type):
        macher2 = RelationshipMatcher(self.driver)
        relationship = macher2.match(r_type=r_type)  # 找出关系类型为KNOWS的关系
        return relationship
    
    # 判断关系是否存在
    def has_relationship(self,firstNode, finalNode, rel):
       query = f"MATCH {firstNode}-[r:{rel}]->{finalNode} RETURN r"
       relationships = self.driver.run(query)
       if len(list(relationships)) > 0:
           print('关系已经存在')
           return True
       else:
           return False
        



def process_file(file_path,education_graph):
    # 对每个文件进行操作
    print(f"处理文件：{file_path}")
    data = json2triple(read_json(file_path)["data"])
    # neo4j操作
    for item in data:
        start_value = None
        end_value = None
        for key, value in item[0].items():
            start_value = [key, {"name": value}]
        for key, value in item[2].items():
            end_value = [key, {"name": value}]
        # print(start_value)
        start_node = education_graph.create_node(start_value[0], start_value[1])
        relationship = item[1]
        end_node = education_graph.create_node(end_value[0], end_value[1])
        education_graph.create_relationship_by_node(start_node, relationship, end_node)


def walk_through_directory(directory_path,education_graph):
    for root, _, files in os.walk(directory_path):
        for file in files:
            file_path = os.path.join(root, file)
            detailed_path = f'data\detailed\{file}'
            # print(detailed_path)
            # if os.path.exists(detailed_path):
            #     print(22222)

            if not os.path.exists(detailed_path):
               # 将处理过的文件放进data\\detailed下即可for循环处理时略过
               process_file(file_path,education_graph)





    
 
if __name__ == '__main__':
    try:
    # neo4j操作
        education_graph = EducationGraph(host="192.168.2.155", port=7687, username="neo4j", password='password')
        # 自动化操作
        # for循环运行某个文件夹
        # walk_through_directory('data\\author',education_graph)



        # 运行某个文件
        data = read_excel2("王\\王\\增加\\工作簿5.xlsx")
        # education_graph = EducationGraph(host="192.168.2.155",port=7687,username="neo4j",password='password')

        for item in data:
            start_value = None
            end_value = None
            for key, value in item[0].items():
                start_value = [key,{"name":value}]
            for key, value in item[2].items():
                end_value = [key, {"name":value}]
            # print(start_value)

            start_node = education_graph.create_node(start_value[0],start_value[1])
            relationship = item[1]
            end_node = education_graph.create_node(end_value[0],end_value[1])
            education_graph.create_relationship_by_node(start_node,relationship,end_node)
            print(f"{start_value[1]['name']}--{relationship}-{end_value[1]['name']}")
    except Exception as e:
        print(e)

    #
    #
    #
    #
    #
    #
    
    
    
    
    
    
    
    
    
    # education_graph = EducationGraph(host="192.168.2.155",port=7687,username="neo4j",password='password')
    # node1=education_graph.create_node("Coder",{"name":"demo5"})
    # node2=education_graph.create_node('Teacher',{'name':'demo6'})
    # education_graph.create_relationship_by_node(node1,"Lover",node2)
    # print(list(education_graph.search_relationships('Lover')))
    # for item in list(education_graph.search_relationships('Lover')):
    #     print(type(item))
    #     print(item)





# 创建关系
# a = Node("Person", name="Alice", sex="female", ID="222")
# b = Node("Person", name="Bob", sex="male", ID="123")
# ab = Relationship(a, "KNOWS", b)
# graph.create(ab)


# 查询节点与关系
# macher1 = NodeMatcher(graph)
# macher2 = RelationshipMatcher(graph)
# node1 = macher1.match("Person", ID="123")  # 匹配ID为123的节点
# node2 = macher2.match(r_type="KNOWS").limit(25)  # 找出关系类型为KNOWS的前25个关系

# result1 = list(node1)
# print(result1)
# result2 = list(node2)
# print(result2)

# 更新节点
 # label2 = "SecuritiesExchange"
        # attrs2 = {"name": "上海证券交易所"}
        # node = match_node(self.driver, label, attrs)  # 找到对应的结点
        # node.update({"name": "北交所", "addr": "北京"})  # 修改结点的属性
        # graph.push(node)  # 更新结点，注意：如果没有这一步，则结点不会被更新