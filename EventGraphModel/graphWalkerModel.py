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

import json
import uuid
from typing import Optional

class Edge:
    '''
       Edge class, used to describe edge for the Model class.
    '''
    def __init__(self, id:str, name: str, source_vertex_id: str, target_vertex_id: str):
        self.id = id
        self.name = name
        self.source_vertex_id = source_vertex_id
        self.target_vertex_id = target_vertex_id

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "sourceVertexId": self.source_vertex_id,
            "targetVertexId": self.target_vertex_id,
        }

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)


class Vertex:
    '''
       Vertex class, used to describe vertex for the Model class.
    '''
    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
        }

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

class GWModel:

    '''
        simpler version of graphwalker model,
        this class has all the information required for the creation of a model
        it also generates json versions of the mode,
        which is the ibput of the Graphwalker mbtester
    '''

    def __init__(self, generator: str, name: str):

        if not generator or not name:
            raise ValueError("Both generator and name must be non-empty.")

        self.generator = generator
        self.id = str(uuid.uuid4())  # Generate a unique ID
        self.name = name
        self.edges = []
        self.vertices = []

    def create_vertex(self, name: str):
        v_id = f"v0" if len(self.vertices) == 0 else f"v{len(self.vertices) + 1}"
        new_vertex = Vertex(id=v_id, name=name)
        self.vertices.append(new_vertex)

    def get_vertex_id_by_name(self, name: str) -> Optional[str]:
        for vertex in self.vertices:
            if vertex.name == name:
                return vertex.id
        return None

    def create_edge(self, name: str, source_vertex: str, target_vertex: Optional[str]):
        e_id = f"e{len(self.edges)}"
        source_vertex_id = self.get_vertex_id_by_name(source_vertex)
        target_vertex_id = "v0" if target_vertex is None else self.get_vertex_id_by_name(target_vertex)
        new_edge = Edge(id=e_id, name=name, source_vertex_id=source_vertex_id, target_vertex_id=target_vertex_id)
        self.edges.append(new_edge)

    def to_dict(self):
        return {
            "generator": self.generator,
            "id": self.id,
            "name": self.name,
            "edges": [edge.to_dict() for edge in self.edges],
            "vertices": [vertex.to_dict() for vertex in self.vertices]
        }

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)
