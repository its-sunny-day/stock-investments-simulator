import os
import datetime
from datapackage import Package

#Load data from specified dataset within a datapackage. Additionally checks the type of the dataset.
def getDataIterator(packagePath, datasetIndex, expectedDatasetType):
    resource = Package(packagePath).resources[datasetIndex]
    datasetType = resource.descriptor['datahub']['type']
    if datasetType != expectedDatasetType:
        print("Error: Expected to find '%s' dataset, but '%s' found with index %d in datapackage '%s'." % (expectedDatasetType, datasetType, datasetIndex, packagePath))
        return None
    return resource.iter()

def getDataIteratorForLocalPackage(relativePackagePath, datasetIndex, expectedDatasetType):
    dirName = os.path.dirname(__file__)
    return getDataIterator(os.path.join(dirName, relativePackagePath), datasetIndex, expectedDatasetType)

#Load SP500 historiacal data
def getSp500Iterator():
    # Dataset 0 contains monthly data
    return getDataIteratorForLocalPackage("data/sp500/datapackage.json", 0, "original")

#Load Gold price history
def getGoldIterator():
    # Dataset 1 contains monthly data
    return getDataIteratorForLocalPackage("data/gold/datapackage.json", 1, "original")

def iterateTillDate(resourceIter, targetDate):
    for line in resourceIter:          
        #Date field has index 0
        date = line[0]
        #Convert '_yearmonth'(named tuple) to date, if needed
        if type(date).__name__.endswith("yearmonth"):
            date = datetime.date(date.year, date.month, 1)            
        if targetDate == date:
            return line    
    return None

def simulatePeriod(startYear, numberOfYears, investmentPerMonth, dataIterator, assetName = "asset", verbose = False):
    moneySpent = 0
    assetsInPortfolio = 0

    startDate = datetime.date(startYear, 1, 1)
    #Iterate till start date of simulation period
    dataItem = iterateTillDate(dataIterator, startDate)
    if dataItem == None:
        print("Unable to find target date(%s)..." % startDate)
        return

    for simulationYear in range(startYear, startYear + numberOfYears + 1):
        for month in range (1, 12 + 1):    
            expectedDate = datetime.date(simulationYear, month, 1)
            itemDate = dataItem[0]
            #Convert '_yearmonth'(named tuple) to date, if needed
            if type(itemDate).__name__.endswith("yearmonth"):
                itemDate = datetime.date(itemDate.year, itemDate.month, 1)  
            if expectedDate != itemDate:
                print("Error: Expected date %s, but %s found..." % (expectedDate, itemDate))
                return
            costOfAsset = float(dataItem[1])
            moneySpent += investmentPerMonth
            newAssets = float(investmentPerMonth) / costOfAsset
            assetsInPortfolio += newAssets
            if verbose:
                print("%s spent %d$, bought stocks %f" %( itemDate, investmentPerMonth, newAssets ))
            #Take next row from historical data            
            dataItem = next(dataIterator)

    print("Period: %s --> %s (%d years)" % (startDate, itemDate, numberOfYears))
    print("Investment per month: %s$" % (investmentPerMonth))
    print("Money spent: %d$" % moneySpent)
    print("Number of %s in portfolio (at the end of simulation period): %.2f" % (assetName, assetsInPortfolio))
    print("Cost of one %s (at the end of simulation period): %d$" % (assetName, costOfAsset))
    currentCostOfPortfolio = round(assetsInPortfolio * costOfAsset)
    print("Cost of portfolio (at the end of simulation period): %d$" % currentCostOfPortfolio)
    profitPerc = (currentCostOfPortfolio - moneySpent) * 100 / moneySpent
    print("Total profit: %.2f%%" %  profitPerc)
    print("Profit per 1 year: %.2f%%" %  float(profitPerc / numberOfYears))
    

#print("Enter number of years to simulate:")
#numberOfYears=int(input())
numberOfYears = 10
#print("Enter start year:")
#startYear=int(input())
startYear = 1989

#print("How much you would like to invest each month,$:")
#investmentPerMonth=int(input())
investmentPerMonth = 100

#Simulate buying of SP500 stocks
print("--------------------------------------")
print("SP500:")
print("--------------------------------------")
simulatePeriod(startYear, numberOfYears, investmentPerMonth, getSp500Iterator(), "SP500")

print("")

print("--------------------------------------")
print("Gold:")
print("--------------------------------------")
simulatePeriod(startYear, numberOfYears, investmentPerMonth, getGoldIterator(), "Gold(troy ounces, oz)")