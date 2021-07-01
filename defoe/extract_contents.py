from argparse import ArgumentParser
import importlib
import json

root_module = "defoe"
setup_module = "setup"
models = ["books", "papers", "fmp", "nzpp", "generic_xml", "nls", "nlsArticles", "phdtheses", "hdfs", "psql", "es"]

def extract(model_name, file_name):
    setup = importlib.import_module(f'{root_module}.{model_name}.{setup_module}')
    obj_err = setup.filename_to_object(file_name)
    if obj_err[1] is not None:
        return
    
    yield from setup.object_as_dict(obj_err[0])

def main():

    parser = ArgumentParser(description="Retrieve contents from an input file and return as a single JSON encoded line")
    parser.add_argument("model_name",
                        help="Data model to which data files conform: " +
                        str(models))
    parser.add_argument("input_file",
                        help="Data file to extract")
    parser.add_argument("--metadata",
                        help="Opional metadata in JSON format to be added to the output",
                        required=False,
                        default='')

    args = parser.parse_args()
    model_name = args.model_name
    file_name = args.input_file
    metadata = json.loads(args.metadata)
    for obj in extract(model_name, file_name):
        obj.update(metadata)
        print(json.dumps(obj))

if __name__ == '__main__':
    main()    
