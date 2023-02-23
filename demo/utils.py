from typing import List, Iterator

from attr import dataclass


@dataclass
class IdVector:
    id: int
    vector: List[float]


def read_from_csv(query_out_path: str) -> Iterator[IdVector]:
    with open(query_out_path) as file:
        line = file.readline()
        while line:
            id_ = int(line[:line.index(',')])
            vector = list(eval(line[line.index('['):line.index(']') + 1]))
            line = file.readline()
            yield IdVector(id=id_, vector=vector)



