from jsonpath_ng import parse


data = {'a':
            {'b':
                 {'c':
                      {'d': 'value'}}}}

expr = parse('$..d')
for match in expr.find(data):
    match.full_path.update(data, 'Updated')
print(data)