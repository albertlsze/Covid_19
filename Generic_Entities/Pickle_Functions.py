import pickle

def save(data,filepath):
    with open(filepath, 'wb') as f:
        pickle.dump(data,f)

def load(filepath):
    with open(filepath, 'rb') as f:
        return pickle.load(f)