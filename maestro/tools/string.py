import re


def replaceall(replace_dict, string):
    """
    replaceall will take the keys in replace_dict and replace string with their corresponding values. Keys can be regular expressions.
    """
    replace_dict = dict((re.escape(k), v) for k, v in list(replace_dict.items()))
    pattern = re.compile("|".join(list(replace_dict.keys())))
    return pattern.sub(lambda m: replace_dict[re.escape(m.group(0))], string)
