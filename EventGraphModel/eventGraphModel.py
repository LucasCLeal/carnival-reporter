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
        self.cluster_topic_data = {}
        self.model_history = {}

    def create_gw_model(self, generator: str, name: str) -> GWModel:
        """
        Factory method to instantiate GWModel only if all parameters are provided.

        Parameters:
        generator (str): The generator string.
        name (str): The name of the model.

        Returns:
        GWModel: An instantiated object of GWModel if parameters are valid.
        None: If any parameter is invalid (optional, depending on design choice).
        """
        if generator and name:  # Ensure neither parameter is None or empty
            return GWModel(generator, name)
        else:
            raise ValueError("Both 'generator' and 'name' must be provided and non-empty.")

    def update_topic_meta_data(self):

        # todo função criada para atualizar as informaçoes sobre os tópicos no cluster
        pass

    def update_from_ConsumerLog_topic(self, data):
        print('TODO update_from_ConsumerLog_topic')
    def update_from_regular_topic(self, data):

        # todo recebe uma mensagem de um dos tópicos do cluster.
        print('TODO update_from_regular_topic')
        #content = json.loads(data)
        #print(content['service'])
        #print(content['consuming'])
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
