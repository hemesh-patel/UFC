from pyspark import SparkContext
from pyspark import SparkConf
from datetime import datetime


def csv(d):
    return ','.join(i for i in d)

sc = SparkContext()


def technique():

    """This function does a reduction by key on the techniques used to win matches. Minus the boring information,
    namely unanimous, majority decisions"""


    #fights1 = sc.textFile('/ufc/fights.txt').map(lambda x: x.split('\t')).map(lambda x: (x[5], x[6], x[9], x[10], x[11],
    #                                                                                 x[12],x[15], x[16], x[17], x[18],
    #                                                                                 x[19]))  # removing unwanted data
    #fights_header = fights1.first()
    #fights2 = fights1.filter(lambda x: x != fights_header)  # remove header
    #fights3 = fights2.map(lambda x: (datetime.strptime(str(x[0]), '%m/%d/%Y'), str(x[1]), str(x[2]), str(x[3]), str(x[4]),
                                 #str(x[5]), str(x[6]), str(x[7]), str(x[8]), int(x[9])))
                                 #datetime.strptime(str(x[10]), '%M:%S')))  # casting the elements
    fights1 = sc.textFile('/ufc/fights.txt').map(lambda x: x.split('\t'))  # splitting by tab
    fights2 = fights1.filter(lambda x: x[16] != 'Unanimous').filter(lambda x: x[16] != '').\
        filter(lambda x: x[16] != 'Majority').filter(lambda x: x[16] != 'method_d')  # filter out rubbish
    """single_tech = fights2.map(lambda x: (x[16], 1)).filter(lambda x: x[1] == 1)  # counts the single techniques used
    fights4 = fights2.map(lambda x: (x[16], 1))
    fights5 = fights4.subtract(single_tech)  #.map(lambda x: (x[1], int(1[0]))).reduceByKey(lambda x, y : x+y)"""
    #When I print the above I get the following error:'WARN DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.'
    #So I just remove the singles techniques manually in excel


    fights3 = fights2.map(lambda x: (x[16], 1)).reduceByKey(lambda x, y: x+y).map(lambda x: (str(x[0]), str(x[1])))  # aggregation

    fights4 = fights3.map(csv)

    try:
        fights4.saveAsTextFile('/ufc/technique')
    except ValueError:
        print 'Error'

# technique()


def ppv():

    """This function looks at the ppv data and splits the ppv value between the two fighters(50/50)and then I do a
    simple reduceByKey to look at how much money each fighter has brought in."""

    ppv1 = sc.textFile('/ufc/ppv.txt').map(lambda x: x.split('\t')).filter(lambda x: x[3] !='Canceled').\
        filter(lambda x: x[2] != 'Sylvia/Nog, Lesnar/Mir').map(lambda x: (x[2].split(' ')[0], int(x[3].replace(',', ''))/2))  # This looks at the first fighter

    ppv2 = sc.textFile('/ufc/ppv.txt').map(lambda x: x.split('\t')).filter(lambda x: x[3] !='Canceled').\
        filter(lambda x: x[2] != 'Sylvia/Nog, Lesnar/Mir').map(lambda x: (str(x[2].split(' ')[2]), int(x[3].replace(',', ''))/2))  # This looks at the 2nd fighter

    ppv3 = ppv1.union(ppv2).reduceByKey(lambda x, y: x+y).sortBy(lambda x: x[1], False).map(lambda x: (str(x[0]), str(x[1])))  # joins the two RDD's

    ppv4 = ppv3.repartition(1).map(csv)
    try:
        ppv4.saveAsTextFile('/ufc/ppv_results')
    except ValueError:
        print 'Error'
# ppv()
def percentage_wins():
    """This function looks at how successful each fighter has been. Answers the following question: how many fights
    they won as a percentage of the number of fights they took part in"""
    fights1 = sc.textFile('/ufc/fights.txt').map(lambda x: x.split('\t'))
    fights2 = fights1.map(lambda x: (x[9], x[11])).filter(lambda x: x[1] == 'win').map(lambda x: (x[0], 1))  # filter out draws
    fights_won = fights2.reduceByKey(lambda x,y: x+y)  # this RDD is the count how many fights each fighter has won

    fights3 = fights1.map(lambda x: (x[9], 1))  # RDD looks like ==> (fighter, 1)
    fights4 = fights1.map(lambda x: (x[10], 1))  # RDD looks like ==> (fighter, 1)
    # the above RDD's takes the fighters name
    fights_total = fights3.union(fights4).reduceByKey(lambda x,y: x+y) # this RDD is the counts of total fights each fighter has  taken part in
    fights_avg = fights_won.join(fights_total).map(lambda x: (x[0], (float(x[1][0])/float(x[1][1])), x[1][1]))\
        .sortBy(lambda x: x[1], False).map(lambda x: (str(x[0]), str(float('%.3f' % x[1])*100) + '%', str(x[2])))
    #RDD looks like ==> (fighter, percentage_wins, fights)
    final = fights_avg.repartition(1).map(csv)

    try:
        final.saveAsTextFile('/ufc/percentage_wins.csv')
    except ValueError:
        print 'Error'
#percentage_wins()


sc.stop()
