import os
import ConfigParser

section = "user_information"
path = "config.cfg"

class Bunch(object):
    def __init__(self, adict):
        self.__dict__.update(adict)


def config_test(args):

    config = ConfigParser.RawConfigParser()

    if os.path.isfile(path):
        config.read(path)

        curr_config = parse_config(config)
        if config.has_section(section) and len(args) <= 0:
            return curr_config
        else:
            return create_config(args, curr_config, config, path)
    else:
        return create_config(args, None, config, path)


def create_config(new_config, curr_config, config_parser, path):

    if curr_config != None:
        for k, v in curr_config.iteritems():
            if not k in new_config:
                new_config[k] = v

    config_file = open(path, "w")

    if not config_parser.has_section(section):
        config_parser.add_section(section)

    for k in new_config:
        config_parser.set(section, k, new_config[k])

    config_parser.write(config_file)
    config_file.close()

    return dict(config_parser.items(section))

def parse_config(config_parser):

    data = config_parser.items(section)

    if len(data) == 0:
        return None

    return dict(data)

def init(args):

    tmp = vars(args)

    final_args = dict((k, v) for k, v in tmp.iteritems() if v)

    config = config_test(final_args)
    if config == None or len(config) < 4:
        return None

    return Bunch(config)
