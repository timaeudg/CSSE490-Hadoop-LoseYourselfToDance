import pandas as pd

hdf = pd.HDFStore(filePath)

for key in store.keys()
    for val in store[key]:
        try:
            jsonArray[val] = store[key][val][0].item()
        except:
            jsonArray[val] = store[key][val][0].item()
            
