# Author: Chang Du
# netID: cd2682
# BigDataHomework1: finished individually

# import
import csv
import sys

# input two values in the terminal
filename = sys.argv[1]
output = sys.argv[2]

# define function: read in csv file
def csvRows(filename):
    with open(filename, 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
            yield row

# define three outcome lists and hashfilter
CustomerCount = []
TotalRevenue = []
ProductId = []
hashFilter = []

for row in csvRows(filename):
    # if the same product has been read in before
    if row['Product ID'] in ProductId:
        # find the index of this product to locate ItemCost and CustomerID
        ind = ProductId.index(row['Product ID'])
        TotalRevenue[ind] = TotalRevenue[ind] + float(row['Item Cost']) # add to TotalRevenue
        
        # define hash filter
        h1 = int(row['Customer ID'])%10 
        h2 = (int(row['Customer ID'])//10)%10
        
        # whether ths Customer had been readin
        # if any of h1 and h2 is not in hashfilter, this Customer is not in the list
        if h1 not in hashFilter[ind][0] or h2 not in hashFilter[ind][1]:
            hashFilter[ind].append([h1,h2]) # add this CustomerID into hushfilter
            CustomerCount[ind] = CustomerCount[ind] + 1 # add this new Customer to CustomerCount
    
    # if this product has not been readin
    else:
        ProductId.append(row['Product ID']) # readin this Product
        TotalRevenue.append(float(row['Item Cost'])) 
        CustomerCount.append(1) # customercount =0+1
        ind_h = ProductId.index(row['Product ID'])
        h1 = int(row['Customer ID'])%10
        h2 = (int(row['Customer ID'])//10)%10
        hashFilter.append([set(), set()]) # initialize hashfilter
        hashFilter[ind_h][0].add(h1) # readin [h1,h2] to hashfilter
        hashFilter[ind_h][1].add(h2)
#print(CustomerCount)
#print(TotalRevenue)
#print(ProductId)

# put the three result lists in one list
result = list()
for i in range(len(ProductId)):
    result.append([ProductId[i], CustomerCount[i], TotalRevenue[i]])
    
# sort the result by PrductID
result.sort(key=lambda x:x[0])

# write the result in a csv file
csvfile = open(output, 'w')
writer = csv.writer(csvfile)
writer.writerow(['Product ID', 'Customer Count', 'Total Revenue'])# header
writer.writerows(result)

csvfile.close()