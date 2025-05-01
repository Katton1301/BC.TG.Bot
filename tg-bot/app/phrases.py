import json

class Phrases:
    def __init__(self, path = 'resources/phrases.json'):
        with open(path, encoding="utf-8") as file:
            self._phrases_dict = json.load(file)
        if len(self._phrases_dict) == 0 or 'ru' not in self._phrases_dict or 'en' not in self._phrases_dict:
            raise Exception(f'Bad data in json file {path}')
        
        self._available_langs = []
        for lang in self._phrases_dict.keys():
            self._available_langs.append(lang)


    def dict(self, name, lang):
        if lang not in self._phrases_dict:
            lang = 'en'
        return self._phrases_dict[lang][name]
    
    def checkPhrase(self, name, text):
        for lang in self._available_langs:
            if self._phrases_dict[lang][name] == text:
                return True
        return False
    
    def langs(self):
        return self._available_langs
    

phrases = Phrases()


