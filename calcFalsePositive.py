def FalsePositive(datanum, count):
    numStr=str(datanum)
    n=len(numStr)-1
    all=10**n
    fp=abs(all-count)/all
    return all,fp