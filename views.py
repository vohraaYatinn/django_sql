import sys
import os
import django


# sys.path.insert(0, "../.")
# os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'updationprocess.settings')
# django.setup()

import time
from django.apps import apps
import pandas as pd
from makeupdates.models import Person
from django.http import HttpResponse




def createProduct(request):
    # Person.objects.all().delete()
    counter=1
    unsaved_files={}
    start = time.time()
    counter_time = 10
    while counter<100000:
        try:
            print(counter)
            reader=pd.read_csv('makeupdates/products.csv', skiprows=range(1, counter), dtype='unicode',low_memory=False)

            # with open('makeupdates/products.csv') as file:
            #     reader = csv.reader(file)
            #     if not counter:
            # print(reader['product_id'])
            for index, row in reader.iterrows():
                person = Person(row['product_id'],row['product_name'],row['category'],row['audit_form'],row['label'],row['therapy_type'],row['therapeutic_action'],row['therapeutic_class_l3'],row['therapeutic_class_l1'],row['therapeutic_class_l2'],row['category_group'])
                person.save()
                counter = counter + 1
                print("saved "+str(counter))
            end = time.time()
            print("Time Taken for Insert data: {} sec".format(end - start))
            print(counter,"records")
        except Exception as e:
            unsaved_files[counter]=e
            counter=counter+1
            print("sleeping")
            time.sleep(counter_time)
        print("---------------------------------------------")
        print(unsaved_files)
        print("---------------------------------------------")

    return HttpResponse("success")
        # person = Person()

def createanother(request):
    start = time.time()
    counter=0
    while counter<30:
        try:
            portfolio1=pd.read_csv('makeupdates/products.csv',skiprows=range(1, counter*10000+10),nrows=10000)
            # portfolio1 = csv.DictReader("makeupdates/products.csv")
            # list_of_dict = list(portfolio1)
            # print(list_of_dict)
            objs = [
            Person(row['product_id'],row['product_name'],row['category'],row['audit_form'],row['label'],row['therapy_type'],row['therapeutic_action'],row['therapeutic_class_l3'],row['therapeutic_class_l1'],row['therapeutic_class_l2'],row['category_group'])
            for index, row in portfolio1.iterrows() 
            ]
            print(objs)
            try:
                msg = Person.objects.bulk_create(objs)
            except Exception as e:
                return HttpResponse(e)
            
            counter=counter+1
            end = time.time()
            print("total data inserted"+str(counter*10000))
            print("Time Taken for Insert data: {} sec".format(end - start))
            objs=[]
        
        except Exception as e:
            print(e)
            return HttpResponse(e)
    return HttpResponse("success")

def update_function(self, data_frame, model, chunk_size=10000):
    start = time.time()

    # lists required
    objects_dict={}
    update_queries = []
    products_obj={}
    counter = 1
    df_length = data_frame[data_frame.columns[0]].count()
    req_model = apps.get_model("makeupdates", model)
    primary_key = req_model._meta.pk.name
    # loop_flag=True
    
    while counter<df_length:
        data_chunk = data_frame[counter * chunk_size:(counter + 1) * chunk_size]
        # portfolio1 = pd.read_csv('makeupdates/jan.csv',skiprows=range(1, counter),nrows=chunk_size)
        products = req_model.objects.filter(primary_key__in=list(data_chunk[0]))
        for i in list(products):
            products_obj[i.product_id]=i
        
        for index, row in data_chunk.iterrows():
            try:
                temp_obj = products_obj[row["ProductID"]]
                temp_obj.mrp_jan = row["MRP"]
                temp_obj.cost_jan = row["CostPrice"]
                update_queries.append(temp_obj)
                counter =  counter + 1
                print("saved",counter)
            except Exception as e:
                objects_dict[row["ProductID"]] = e
                counter =  counter + 1

        print("test")

        Person.objects.bulk_update(update_queries, ['mrp_jan','cost_jan'])
        update_queries = []
        products_obj={}
        print("saved batch",counter/10000)
    end = time.time()
        # bulk_update(update_queries, update_fields=['mrp_jan','cost_jan'])
    print("Time Taken for Insert data: {} sec".format(end - start))
    print("success")
    print(objects_dict)
    return HttpResponse("success")



