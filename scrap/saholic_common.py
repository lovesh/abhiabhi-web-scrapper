def get_specs_main(doc,path):
    specs=doc.getNodesWithXpath(path)
    dictionary={}
    for spec in specs:
        clas=spec.get('class')
        if clas=='mainFeature helpdoc' or clas=='mainFeature':
            key=spec.text_content().strip().lower()
            continue
        if clas=='mainFeatureValue' or clas=='mainFeatureValue helpdoc':
            value=spec.text_content().strip().lower()
            if value=='':
                continue
            if key in dictionary:
                if type(dictionary[key])==list:
                    dictionary[key].append(value)
                else:
                    dictionary[key]=[dictionary[key],value]
            else:
                dictionary[key]=value
    
    return dictionary

def get_specs_sub(doc,path,main_text):
    specs=doc.getNodesWithXpath(path)
    dictionary={}
    for spec in specs:
        clas=spec.get('class')
        if clas=='mainFeature helpdoc' or clas=='mainFeature':
            text=spec.text_content().strip()
            if text==main_text:
                flag=1
            else:
                flag=0
            continue
        if (clas=='subFeature helpdoc' or clas=='subFeature') and flag==1:
            key=spec.text_content().strip().lower()
            continue
        if (clas=='subFeatureValue' or clas=='subFeatureValue helpdoc') and flag==1:
            value=spec.text_content().strip().lower()
            if value=='':
                continue
            if key in dictionary:
                if type(dictionary[key])==list:
                    dictionary[key].append(value)
                else:
                    dictionary[key]=[dictionary[key],value]
            else:
                dictionary[key]=value
    return dictionary


