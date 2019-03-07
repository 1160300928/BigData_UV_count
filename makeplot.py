import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
from math import log2
from scipy.interpolate import spline
from scipy import interpolate
import matplotlib


def drawAll():
    #全体情况
    path1='result_all.txt'
    x=[1,2,3,4,5]
    xnew=np.linspace(x[0],x[4],300)
    y1=[]
    z1=[]
    y2=[]
    z2=[]
    y3=[]
    z3=[]
    y4=[]
    z4=[]
    y5=[]
    z5=[]
    y6=[]
    z6=[]
    with open(path1, mode='r+') as fr1:
        lines=fr1.readlines()
        z1=lines[0].strip('\n').split(',')
        y1=lines[1].strip('\n').split(',')
        z2=lines[2].strip('\n').split(',')
        y2=lines[3].strip('\n').split(',')
        z3 = lines[4].strip('\n').split(',')
        y3 = lines[5].strip('\n').split(',')
        z4 = lines[6].strip('\n').split(',')
        y4 = lines[7].strip('\n').split(',')
        z5 = lines[8].strip('\n').split(',')
        y5 = lines[9].strip('\n').split(',')
        z6 = lines[10].strip('\n').split(',')
        y6 = lines[11].strip('\n').split(',')


        all = [y1, z1, y2, z2, y3, z3, y4, z4, y5, z5, y6, z6]
        all_y = [y1, y2, y3, y4, y5, y6]
        all_z=[z1,z2,z3,z4,z5,z6]
        all_ysmooth=[]
        color= ['green','yellow','red','blue','black','purple']
        label=['Sampling','OptmSampling','BF','OptmBF','HLL','HLL++']

        # for i in range(0,len(x)):
        #     all_ysmooth.append(spline(x, all_y[i],xnew))

        for i in range(len(z1)):
            z1[i]=  float(z1[i])
            z1[i]=  log2(z1[i])
            y1[i] = float(y1[i])
            z2[i] = float(z2[i])
            z2[i] = log2(z2[i])
            y2[i] = float(y2[i])
            z3[i] = float(z3[i])
            z3[i] = log2(z3[i])
            y3[i] = float(y3[i])
            z4[i] = float(z4[i])
            z4[i] = log2(z4[i])
            y4[i] = float(y4[i])
            z5[i] = float(z5[i])
            z5[i] = log2(z5[i])
            y5[i] = float(y5[i])
            z6[i] = float(z6[i])
            z6[i] = log2(z6[i])
            y6[i] = float(y6[i])


    func1 = interpolate.interp1d(x, y1, kind='cubic')
    y1=func1(xnew)
    func2 = interpolate.interp1d(x, y2, kind='cubic')
    y2 = func2(xnew)
    func3 = interpolate.interp1d(x, y3, kind='cubic')
    y3 = func3(xnew)
    func4 = interpolate.interp1d(x, y4, kind='cubic')
    y4 = func4(xnew)
    func5 = interpolate.interp1d(x, y5, kind='cubic')
    y5 = func5(xnew)
    func6 = interpolate.interp1d(x, y6, kind='cubic')
    y6= func6(xnew)

    func7 = interpolate.interp1d(x, z1, kind='cubic')
    z1 = func7(xnew)
    func8 = interpolate.interp1d(x, z2, kind='cubic')
    z2 = func8(xnew)
    func9 = interpolate.interp1d(x, z3, kind='cubic')
    z3 = func9(xnew)
    func10 = interpolate.interp1d(x, z4, kind='cubic')
    z4 = func10(xnew)
    func11 = interpolate.interp1d(x, z5, kind='cubic')
    z5 = func11(xnew)
    func12 = interpolate.interp1d(x, z6, kind='cubic')
    z6 = func12(xnew)

    # for i in range(1,13):
    #     plt.subplot(6,2,i)
    #     plt.plot(x,all[i-1])

    plt.figure()
    plt.title('Error_Rate Analysis')
    plt.ylabel('FP_rate(%)')
    plt.xlabel('Quantity')

    plt.plot(xnew,y1,label='Adaptive-Samp')
    plt.plot(xnew,y2,label='Sequential-Samp')
    plt.plot(xnew, y3, label='OptmBF')
    plt.plot(xnew, y4, label='BF')
    plt.plot(xnew, y5, label='HLL')
    plt.plot(xnew, y6, label='HLL++')

    # for i in range(0,6):
    #     plt.plot(xnew,all_y[i],color=color[i],label=label[i])
    plt.legend()

    plt.figure()
    plt.title('Time_Result Analysis')
    plt.ylabel('log2(Time)/(s)')
    plt.xlabel('Quantity')
    plt.plot(xnew, z1, label='Adaptive-Samp')
    plt.plot(xnew, z2, label='Sequential-Samp')
    plt.plot(xnew, z3, label='OptmBF')
    plt.plot(xnew, z4, label='BF')
    plt.plot(xnew, z5, label='HLL')
    plt.plot(xnew, z6, label='HLL++')
    plt.legend()

    plt.show()
    # plt.close()


def drawBF():
    path2 = 'result_bf.txt'
    x = [ 2, 3, 4, 5, 6,7,8,9,10,11]
    xnew = np.linspace(x[0], x[9], 300)
    with open(path2, mode='r+') as fr2:
        lines = fr2.readlines()
        y = lines[1].strip('\n').split(',')
        z = lines[3].strip('\n').split(',')
    func1 = interpolate.interp1d(x, y, kind='cubic')
    y = func1(xnew)
    func2 = interpolate.interp1d(x, z, kind='cubic')
    z = func2(xnew)

    plt.figure()
    plt.title('BF--K--error_rate (size=10000000)')
    plt.xlabel('K')
    plt.ylabel('error_rate(%)')
    plt.plot(xnew,y)

    plt.figure()
    plt.title('BF--K--time (size=10000000)')
    plt.xlabel('K')
    plt.ylabel('Time(s)')
    plt.plot(xnew,z)

    plt.show()

def drawHLL():
    path3='result_hll.txt'
    x = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12,13,14,15,16]
    xnew = np.linspace(x[0], x[14], 300)
    with open(path3, mode='r+') as fr2:
        lines = fr2.readlines()
        y = lines[1].strip('\n').split(',')
        z = lines[3].strip('\n').split(',')
    func1 = interpolate.interp1d(x, y, kind='cubic')
    y = func1(xnew)
    func2 = interpolate.interp1d(x, z, kind='cubic')
    z = func2(xnew)

    plt.figure()
    plt.title('HLL--b--error_rate(size=10000000)')
    plt.xlabel('b')
    plt.ylabel('error_rate(%)')
    # plt.gca().invert_yaxis()
    plt.plot(xnew,y,c='red')

    plt.figure()
    plt.title('HLL--b--time(size=10000000)')
    plt.xlabel('b')
    plt.ylabel('time(s)')
    plt.plot(xnew, z,c='red')
    plt.show()

def drawSP():
    path4 = 'result_sp.txt'
    x= [1,2,3,4,5]
    xnew = np.linspace(x[0], x[4], 300)
    x2=[0.80 ,0.85 ,0.90 ,0.95]
    xnew2=np.linspace(x2[0], x2[3], 300)
    x3=[1,2,3,4]
    xnew3= np.linspace(x3[0], x3[3], 300)

    with open(path4, mode='r+') as fr2:
        lines=fr2.readlines()
        z1 = lines[1].strip('\n').split(',')
        y1 = lines[2].strip('\n').split(',')
        z2 = lines[4].strip('\n').split(',')
        y2 = lines[5].strip('\n').split(',')
        z3 = lines[7].strip('\n').split(',')
        y3 = lines[8].strip('\n').split(',')
        z4 = lines[10].strip('\n').split(',')
        y4 = lines[11].strip('\n').split(',')


    for i in range(len(z1)):
        z1[i] = float(z1[i])
        # z1[i] = log2(z1[i])
        y1[i] = float(y1[i])
        z2[i] = float(z2[i])
        # z2[i] = log2(z2[i])
        y2[i] = float(y2[i])
        z3[i] = float(z3[i])
        # z3[i] = log2(z3[i])
        y3[i] = float(y3[i])
        z4[i] = float(z4[i])
        # z4[i] = log2(z4[i])
        y4[i] = float(y4[i])
    func1 = interpolate.interp1d(x, y1, kind='cubic')
    y1 = func1(xnew)
    func2 = interpolate.interp1d(x, y2, kind='cubic')
    y2 = func2(xnew)
    func3 = interpolate.interp1d(x, y3, kind='cubic')
    y3 = func3(xnew)
    func4 = interpolate.interp1d(x, y4, kind='cubic')
    y4 = func4(xnew)
    func5 = interpolate.interp1d(x, z3, kind='cubic')
    z3 = func5(xnew)
    func6 = interpolate.interp1d(x, z4, kind='cubic')
    z4 = func6(xnew)
    func7 = interpolate.interp1d(x, z1, kind='cubic')
    z1 = func7(xnew)
    func8 = interpolate.interp1d(x, z2, kind='cubic')
    z2 = func8(xnew)


    plt.figure()
    plt.title('Sampling--error_rate')
    plt.xlabel('Quantity')
    plt.ylabel('Error_rate/%')
    plt.plot(xnew, y1,label='Fixed Size')
    plt.plot(xnew, y2,label='Fixed Rate')
    plt.plot(xnew, y3, label='Adaptive-Sampling')
    plt.plot(xnew, y4, label='Sequntial-Sampling')
    plt.legend()

    plt.figure()
    plt.title('Sampling--time')
    plt.xlabel('Quantity')
    plt.ylabel('Time/s')
    plt.plot(xnew, z1, label='Fixed Size')
    plt.plot(xnew, z2, label='Fixed Rate')
    plt.plot(xnew, z3, label='Adaptive-Normal')
    plt.plot(xnew, z4, label='Sequential-Sampling')
    plt.legend()

    plt.show()

def drawAdaptive():
    path4 = 'result_sp.txt'
    x2 = [0.80, 0.85, 0.90, 0.95]
    xnew2 = np.linspace(x2[0], x2[3], 300)
    x3 = [1, 2, 3, 4]
    xnew3 = np.linspace(x3[0], x3[3], 300)
    with open(path4, mode='r+') as fr2:
        lines=fr2.readlines()
        z5 = lines[13].strip('\n').split(',')
        z6 = lines[15].strip('\n').split(',')
        z7 = lines[16].strip('\n').split(',')
        y8 = lines[18].strip('\n').split(',')
        y9 = lines[19].strip('\n').split(',')

    func9 = interpolate.interp1d(x2, z5, kind='cubic')
    z5 = func9(xnew2)
    func10 = interpolate.interp1d(x3, z6, kind='cubic')
    z6 = func10(xnew3)
    func11 = interpolate.interp1d(x3, z7, kind='cubic')
    z7 = func11(xnew3)
    func12 = interpolate.interp1d(x3, y8, kind='cubic')
    y8 = func12(xnew3)
    func13 = interpolate.interp1d(x3, y9, kind='cubic')
    y9 = func13(xnew3)

    plt.figure()
    plt.title('adaptive-p-error_rate')
    plt.xlabel('confidence-p')
    plt.ylabel('error_rate/%')
    plt.plot(xnew2, z5)
    plt.legend()

    plt.figure()
    plt.title('adpt-GEE/normal--error_rate skewed')
    plt.xlabel('Quantity')
    plt.ylabel('error_rate/%')
    plt.plot(xnew3, z6, label='GEE')
    plt.plot(xnew3, z7, label='normal')
    plt.legend()

    plt.figure()
    plt.title('adpt-GEE/normal--time skewed')
    plt.xlabel('Quantity')
    plt.ylabel('time/s')
    plt.plot(xnew3, y8, label='GEE')
    plt.plot(xnew3, y9, label='normal')
    plt.legend()

    plt.show()


if __name__ == '__main__':
    drawAll()
    drawSP()
    drawAdaptive()
    drawBF()
    drawHLL()

    plt.close()
