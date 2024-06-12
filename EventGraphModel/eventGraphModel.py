'''
Copyright (c) 2024 Unicamp

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
OR OTHER DEALINGS IN THE SOFTWARE.

'''
from typing import Optional

from EventGraphModel.graphWalkerModel import Vertex
from EventGraphModel.graphWalkerModel import GWModel
import json
from typing import Dict, Any



class EventGraphModelManager:
    '''
        This class converts the data from the kafka cluster
        and stores the information required to generate graphwalker modes.
        It also holds important information not represented in the GW model that
        can be use to improve the test case generation or system validatation
    '''

    def __init__(self):

        self.name = None
        self.generator = None
        self.kafka_config = {}
        self.topics_data = {}
        self.model_history = []

    def create_gw_model(self) -> GWModel:
        """
        Factory method to instantiate GWModel only if all parameters are provided.

        Parameters:
        generator (str): The generator string.
        name (str): The name of the model.

        Returns:
        GWModel: An instantiated object of GWModel if parameters are valid.
        None: If any parameter is invalid (optional, depending on design choice).
        """
        if self.generator and self.name:  # Ensure neither parameter is None or empty
            model = GWModel(self.generator, self.name)

            #crete vertices and edges for each webservice listed
            ws_set = self._set_of_all_consumers_and_producers()
            if ws_set:
                #creating vertex and edges
                for ws in ws_set:
                    #creating vertex for each ws listed
                    model.create_vertex(str(ws))
                for ws in ws_set:
                    #create edges between ws using topics as ref
                    tp_cons_dic = self._dic_of_consumers_by_topic(ws)
                    for tp in tp_cons_dic:
                        for cons in tp_cons_dic[tp]:
                            model.create_edge(tp,ws,cons)
            return model
        else:
            raise ValueError("Both 'generator' and 'name' must be provided and non-empty.")

    def update_topic_meta_data(self):

        # todo função criada para atualizar as informaçoes sobre os tópicos no cluster
        pass

    def update_from_consumer_log_topic(self, message):
        """
        This function receives the records sent to the topic ConsumerLog.
        The messages in this topic are used to register which topics the services are consuming.
        """
        try:
            data = json.loads(message)
        except Exception as ex:
            print("update_from_ConsumerLog_topic", ex)
            return

            # Extract information with default fallbacks
        topic = data.get('topic', '')
        consumer = data.get('consumer', '')
        action = data.get('action', '')

        if not topic or not consumer or not action:
            print("update_from_ConsumerLog_topic - missing necessary data:", data)
            return

        # Update cluster data based on action
        if topic in self.topics_data:
            if action == "add":
                self.topics_data[topic].setdefault("consumers", set()).add(consumer)
            elif action == "remove":
                if consumer in self.topics_data[topic].get("consumers", set()):
                    self.topics_data[topic]["consumers"].remove(consumer)
                else:
                    print(f"consumers {consumer} not found in topic {topic}")
        else:
            if action == "add":
                self.topics_data[topic] = {"consumers": {consumer}}
            else:
                print("update_from_ConsumerLog_topic - no existing topic for removal:", data)

    def update_from_producer_log_topic(self, message):
        """
        It is designed to manage and update a data structure (self.topics_data)
        that tracks producers associated with specific topics in a Kafka cluster.
        This function processes messages from a Kafka topic called ProducerLog,
        which contains records of actions (like adding or removing producers) taken on various topics.
        """

        try:
            data = json.loads(message)
        except Exception as ex:
            print("update_from_ProducerLog_topic",ex)
            return

        # Extract information with default fallbacks
        topic = data.get('topic', '')
        producer = data.get('producer', '')
        action = data.get('action', '')

        if not topic or not producer or not action:
            print("update_from_ProducerLog_topic - missing necessary data:", data)
            return

        # Update cluster data based on action
        if topic in self.topics_data:
            if action == "add":
                self.topics_data[topic].setdefault("producers", set()).add(producer)
            elif action == "remove":
                if producer in self.topics_data[topic].get("producers", set()):
                    self.topics_data[topic]["producers"].remove(producer)
                else:
                    print(f"Producer {producer} not found in topic {topic}")
        else:
            if action == "add":
                self.topics_data[topic] = {"producers": {producer}}
            else:
                print("update_from_ProducerLog_topic - no existing topic for removal:", data)

    def _set_of_all_consumers_and_producers(self):

        full_set = {}
        if self.topics_data:
            return None
        for topic in self.topics_data:
            cons = self.topics_data[topic]["consumers"]
            prod = self.topics_data[topic]["producers"]
            full_set.update(cons)
            full_set.update(prod)
        return full_set

    def _dic_of_consumers_by_topic(self,producer):

        #list all the topics that recieve msgs from the producer
        topic_list = {tp for tp in self.topics_data if producer in self.topics_data[tp]["producers"]}
        if len(topic_list) == 0:
            return None

        #using the topics, now list all the consumers of the given producer
        consumer_dic = {}
        for tp in topic_list:
            consumer_dic[tp]=self.topics_data[tp]["consumers"]
        return consumer_dic

    def create_vertex_from_topic_record(self, data):

        # todo criar um vertice no GWmodel que vai presentar um serviço
        pass

    def create_edge_from_topic(self, topic: str, source_vertex: Vertex, target_vertex: Optional[Vertex]):
        event_count = self.topic_info.get(topic, 0)
        edge_name = f"{topic} (events: {event_count})"
        self.create_edge(name=edge_name, source_vertex=source_vertex, target_vertex=target_vertex)

    def to_dict(self):
        base_dict = super().to_dict()
        base_dict["topicInfo"] = self.topic_info
        return base_dict

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

    def compare_models(self, md1: GWModel, md2: GWModel | None) -> Dict[str, Any]:
        def compare_lists(list1, list2, key):
            dict1 = {item[key]: item for item in list1}
            dict2 = {item[key]: item for item in list2}

            added = [dict2[k] for k in dict2 if k not in dict1]
            removed = [dict1[k] for k in dict1 if k not in dict2]
            modified = [dict2[k] for k in dict2 if k in dict1 and dict2[k] != dict1[k]]

            return {"added": added, "removed": removed, "modified": modified}

        m1_dict = md1.to_dict()
        m2_dict = md2.to_dict()
        #m2_dict = md2.to_dict() if md2 else self.model_history[-1]["model"]

        vertex_diff = compare_lists(m1_dict["vertices"], m2_dict["vertices"], "name")
        edge_diff = compare_lists(m1_dict["edges"], m2_dict["edges"], "name")

        has_differences = (
                bool(vertex_diff["added"]) or
                bool(vertex_diff["removed"]) or
                bool(vertex_diff["modified"]) or
                bool(edge_diff["added"]) or
                bool(edge_diff["removed"]) or
                bool(edge_diff["modified"])
        )

        return {
            "vertices": vertex_diff,
            "edges": edge_diff,
            "has_differences": has_differences
        }
