from look2bq.executors import YamlExecuter

yaml_path = "events_config.yaml"
exe = YamlExecuter(yaml_path)
exe.execute()