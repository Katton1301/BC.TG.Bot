import json

class Phrases:
    def __init__(self, path = 'resources/phrases.json'):
        with open(path, encoding="utf-8") as file:
            self._phrases_dict = json.load(file)
        if len(self._phrases_dict) == 0 or 'ru' not in self._phrases_dict or 'en' not in self._phrases_dict:
            raise Exception(f'Bad data in json file {path}')


    def dict(self, name, lang):
        return self._phrases_dict[lang][name]
    

phrases = Phrases()


