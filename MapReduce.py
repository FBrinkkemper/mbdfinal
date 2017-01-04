import json

class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

        self.map_input_size = 0
        self.map_output_size = 0
        self.reduce_input_size = 0
        self.reducer_max_size = 0
        self.output_size = 0

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)
        self.map_output_size += 1

    def emit(self, value):
        self.result.append(value) 
        self.output_size += 1

    def execute(self, data, mapper, reducer):
        for line in data:
            pair = json.loads(line)
            self.map_input_size += 1
            mapper(pair[0], pair[1])

        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        # a sorting step, only for pretty printing and easy grading
        self.result.sort()

        #jenc = json.JSONEncoder(encoding='latin-1')
        jenc = json.JSONEncoder()
        for item in self.result:
            print jenc.encode(item)

        self.reduce_input_size = len(self.intermediate)
        for i in self.intermediate.values():
            if len(i) > self.reducer_max_size:
                self.reducer_max_size = len(i)

        print "\n--> Complexity metrics for your program:"
        print "--> Input size to Map tasks (in number of key-value pairs): ", self.map_input_size
        print "--> Replication rate: ", float(self.map_output_size) / self.map_input_size
        print "--> Input size to Reduce tasks (in number of key-value pairs): ", self.reduce_input_size
        print "--> Reducer max size (in number of values): ", self.reducer_max_size
        print "--> Output size (in number of values): ", self.output_size
