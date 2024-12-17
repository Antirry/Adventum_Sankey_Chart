import jinja2
import os

def render_def(name_yml: str, name_query:str, name_folder:str):
    folder_file = os.path.abspath(__file__)
    folder = os.path.dirname(folder_file) + '\\'
    parent_folder = folder.rsplit('\\', 2)[0] + '\\'

    templateLoader = jinja2.FileSystemLoader(searchpath=folder + name_folder)
    templateEnv = jinja2.Environment(loader=templateLoader)

    def var(key) -> any:
        import yaml
        with open(parent_folder + name_yml, 'r', encoding='utf-8') as stream:
            try:
                yaml_project = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return yaml_project['vars'][key]
        
    templateEnv.globals.update(var=var)

    template_name = name_query

    template = templateEnv.get_template(template_name)

    render = template.render()

    return render
