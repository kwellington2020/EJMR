from detoxify import Detoxify
detox = Detoxify('multilingual')

# each model takes in either a string or a list of strings
def count_then_measure_post(content: str):
    if len(content.split()) >= 512:
        return {"toxicity": -1, "severe_toxicity": -1, "obscene": -1, "threat": -1, "insult": -1, "identity_attack": -1, "sexual_explicit": -1}
    else:
        to_return = {}
        for k,v in detox.predict(content).items():
            to_return[k] = v.item()

        return to_return
