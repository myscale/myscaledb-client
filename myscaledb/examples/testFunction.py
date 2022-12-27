import asyncio
from myscaledb import AsyncClient, Client
# from myscaledb.async_db import AsyncClient
from aiohttp import ClientSession

search_vector = [0.3649769425392151, -0.3007376492023468, -0.1850794553756714, -0.06350094079971313, 0.13993972539901733, 0.2808823585510254, -0.028814559802412987, -0.18522828817367554, -0.38216033577919006, -0.332410991191864, 0.12451352179050446, 0.36680394411087036, 0.09484779834747314, -0.08524223417043686, 0.15063349902629852, 0.4242963194847107, -0.35728147625923157, 0.2431757003068924, -0.21603277325630188, 0.05785447731614113, -0.3291065990924835, 0.34372279047966003, -0.33943602442741394, -0.4617447853088379, 0.18424326181411743, 0.24263529479503632, -0.043345604091882706, -0.04484318941831589, -0.0380106158554554, 0.07849941402673721, -0.3420898914337158, -0.5363072752952576, 0.05306820198893547, -0.11726805567741394, 0.020708873867988586, 0.009539924561977386, 0.06356054544448853, 0.45051172375679016, 0.14550533890724182, -0.14122900366783142, 0.06441308557987213, -0.12386608123779297, -0.2626078724861145, -0.03280523419380188, -0.46263161301612854, -0.28133872151374817, 0.20866920053958893, 0.011479459702968597, -0.0833262950181961, 0.34637215733528137, -0.020702898502349854, -0.386075884103775, -0.12817366421222687, -0.5562903881072998, -0.0681612491607666, -0.16181913018226624, 0.1337873935699463, 0.1400701403617859, -0.17294561862945557, -0.21510884165763855, -0.06867150962352753, 0.12465886771678925, 0.09678390622138977, 0.20287741720676422, 0.3142779469490051, 0.30838221311569214, 0.22609931230545044, -0.03016107901930809, 0.24455569684505463, 0.18195639550685883, 0.20721310377120972, 0.0037731491029262543, 0.30912214517593384, -0.2785533368587494, -0.1053394004702568, -0.5158126354217529, -0.0034512095153331757, 0.405829519033432, 0.22006995975971222, 0.13869215548038483, -0.16839741170406342, -0.23689310252666473, 0.011074133217334747, 0.3410107493400574, -0.1136666089296341, -0.08074157685041428, -0.485531210899353, 0.4889305531978607, -0.17194634675979614, 0.15053540468215942, 0.132415309548378, 0.018176574259996414, -0.4388997554779053, -0.3399595618247986, 0.08067530393600464, 0.021665766835212708, -0.17410635948181152, 0.2496451437473297, -0.7499107718467712, -0.20730756223201752, 0.41044628620147705, 0.013133116066455841, -0.08149909973144531, 0.1060139387845993, -0.40782421827316284, 0.14601565897464752, -0.25832515954971313, -0.07468142360448837, 0.09581509977579117, -0.3119922876358032, -0.012705236673355103, 0.3090820014476776, 0.1953539252281189, -0.32791587710380554, -0.26427847146987915, -0.49345558881759644, 0.01588362455368042, 0.0851999968290329, 0.30633795261383057, -0.07661699503660202, -0.3337593674659729, 0.003398948349058628, -0.03192770481109619, 0.4609106481075287, 0.056954506784677505, -0.15600056946277618, -0.3562881052494049, -0.05048888176679611, -0.41894087195396423, 0.27196353673934937, -0.005691997706890106, 0.006433218717575073, -0.38058704137802124, -0.3685697913169861, 0.2203010618686676, 0.09518358111381531, -0.45661383867263794, -0.08993372321128845, 0.22357185184955597, 0.43337512016296387, -0.22342771291732788, 0.3623808026313782, -0.2065090537071228, -0.18992450833320618, -0.2728494107723236, 0.29412829875946045, 0.557199239730835, -0.10232722759246826, -0.02639969065785408, -0.41473740339279175, -0.1301584094762802, 0.0699017196893692, 0.6230918765068054, -0.0438799113035202, 0.13127955794334412, 0.35280758142471313, 0.036938101053237915, -0.0325891338288784, -0.14531898498535156, 0.022602012380957603, -0.07693600654602051, 0.24530670046806335, -0.09244848042726517, 0.05303320288658142, -0.354108065366745, 0.13906510174274445, -0.007073249667882919, 0.40493226051330566, 0.053342461585998535, -0.3573606610298157, 0.5238996744155884, -0.0006318259984254837, -0.26855698227882385, -0.20207616686820984, 0.3080708682537079, 0.09112020581960678, 0.12542051076889038, 0.48774975538253784, 0.015201658010482788, 0.13347241282463074, 0.2587190866470337, 0.26231786608695984, -0.4243960678577423, -0.21761424839496613, 0.04894775152206421, -0.48180609941482544, -0.08339300006628036, 0.08749846369028091, -0.5024474263191223, -0.24538080394268036, 0.12213544547557831, -0.20900659263134003, -0.08739197254180908, -0.19368425011634827, -0.08061140030622482, -0.0399395152926445, -0.15374614298343658, -0.2504522502422333, -0.3428957462310791, -0.028654180467128754, -0.12654772400856018, -0.5036919116973877, -0.2217593938112259, 0.25041988492012024, 0.011194405145943165, 0.357166051864624, -0.042055778205394745, -0.21818102896213531, 0.017978552728891373, -0.20304805040359497, -0.1382116973400116, 0.14345908164978027, 0.049712587147951126, 0.5080094337463379, -0.17824521660804749, -0.14564265310764313, -0.3167755901813507, 0.01737874746322632, -0.04414290562272072, 0.19328013062477112, 0.1964072287082672, 0.022503621876239777, -6.493143558502197, -0.3040974736213684, 0.12573416531085968, -0.015513874590396881, -0.3886408507823944, 0.018155356869101524, -0.09227804839611053, -0.3098708689212799, -0.020700380206108093, 0.33726176619529724, -0.1377367377281189, 0.0764206275343895, -0.02551594376564026, 0.31395724415779114, -0.36354267597198486, -0.23611417412757874, -0.09498534351587296, 0.32246479392051697, -0.1135551854968071, -0.0762370228767395, -0.001157950609922409, -0.6379426121711731, 0.25371500849723816, 0.19413010776042938, 0.26549237966537476, 0.14394813776016235, 0.13293759524822235, -0.03309709578752518, 0.46098315715789795, -0.2540965676307678, 0.2217317819595337, -0.23896217346191406, -0.027709685266017914, -0.1656169593334198, 0.13903707265853882, 0.6799906492233276, -0.24043264985084534, 0.009464278817176819, -0.13011939823627472, 0.24501490592956543, 0.1716289520263672, -0.28452128171920776, 0.0044147297739982605, 0.022204479202628136, 0.04219011962413788, 0.564791202545166, 0.006752923130989075, -0.14780668914318085, -0.13880211114883423, 0.2059657871723175, 0.32607775926589966, -0.11853034049272537, 0.049101006239652634, -0.08629319071769714, -0.21572726964950562, -0.2913740277290344, -0.16637074947357178, 0.2523665130138397, 0.02482764795422554, -0.545268177986145, 0.08842151612043381, -0.6243101358413696, 0.25946667790412903, 0.14283117651939392, 0.21845972537994385, 0.0013123899698257446, 0.12256371974945068, 0.35899168252944946, 0.0820842757821083, 0.10542817413806915, -0.13433487713336945, -0.46943342685699463, 0.3367498517036438, 0.2640068233013153, -0.032179661095142365, -0.012985963374376297, -0.04467393830418587, -0.06215329468250275, -0.1795365810394287, -0.4660935699939728, 0.06230754405260086, -0.12274734675884247, -0.24087607860565186, -0.008656821213662624, 0.022204916924238205, 0.393530935049057, 0.14750917255878448, -0.13217763602733612, -0.23024201393127441, -0.3308000862598419, 0.055648304522037506, 0.10377831757068634, 0.2915692627429962, 0.30818280577659607, 0.17589004337787628, -0.16157668828964233, 0.07089976221323013, 0.011930122971534729, -0.1114693135023117, -0.1950705349445343, 0.054203443229198456, 0.1671491116285324, -0.21793870627880096, 0.18925169110298157, 0.14343923330307007, -0.1252516508102417, -0.3700656592845917, 0.05962957441806793, -0.26847681403160095, -0.05040915310382843, -0.3284483551979065, 0.335845410823822, -0.25489291548728943, -0.21468552947044373, 0.11552754789590836, -0.08259771019220352, -0.20604577660560608, -0.16571462154388428, -0.05203372985124588, -0.33934593200683594, 0.05235837399959564, 0.24057820439338684, 0.32448017597198486, 0.25806182622909546, 0.07493814826011658, -0.12314410507678986, -0.2698586583137512, -0.20866885781288147, 0.007282882928848267, 0.160936176776886, 0.08799047768115997, -0.18287687003612518, 0.49005600810050964, 0.06881804019212723, 0.04443491995334625, 0.4922088086605072, -0.31922003626823425, -0.10070230066776276, 0.25033313035964966, -0.2940562963485718, 0.1271127164363861, -0.040102407336235046, -0.33142030239105225, -0.1061738058924675, 0.18267735838890076, 0.167194664478302, -0.056210894137620926, 0.2128976285457611, -0.14492332935333252, 0.02273884415626526, 0.048767805099487305, -0.52672278881073, 0.2687716782093048, 0.11722248792648315, 0.13824565708637238, 0.19126299023628235, 0.6312925219535828, -0.1420416682958603, 0.12161049991846085, 0.3454475402832031, 0.4113801121711731, -0.19860687851905823, 0.14855435490608215, 0.037389859557151794, -0.20959556102752686, 0.6933825612068176, -0.21049390733242035, -0.34154248237609863, -0.6137647032737732, 0.01270005851984024, -0.14910095930099487, 0.03611736372113228, -0.27828603982925415, 0.5319169759750366, 0.24776452779769897, -0.11202912777662277, -0.1485200971364975, 0.11631793528795242, -0.3267616033554077, -0.2549743950366974, -0.1192471832036972, -0.22378098964691162, -0.11398826539516449, 0.2963868975639343, -0.3198525607585907, 0.2435189187526703, 0.1893755942583084, -0.12828074395656586, -0.27639949321746826, 0.1355155110359192, 0.10486088693141937, 0.28909072279930115, 0.44479483366012573, 0.11612272262573242, 0.3384109139442444, 0.26775193214416504, -0.01969628781080246, -0.1057794913649559, 0.10473459959030151, -0.033040858805179596, 0.010118651203811169, -0.1892140805721283, -0.23599915206432343, 0.06619400531053543, 0.36450403928756714, -0.1040579155087471, -0.1018400564789772, 0.10156260430812836, 0.004558242857456207, 0.34711170196533203, -0.35045596957206726, -0.047235410660505295, 0.0694158673286438, 0.4781867563724518, 0.18195490539073944, -0.2624996304512024, -0.08735014498233795, 0.06829756498336792, -0.21065208315849304, -0.07324196398258209, 0.25203514099121094, 0.3208037316799164, -0.14165180921554565, -0.06974604725837708, 0.41415825486183167, -0.22877740859985352, 0.2448044717311859, 0.12689799070358276, -0.4185529351234436, -0.17126962542533875, -0.47565367817878723, -0.16728737950325012, -0.14347262680530548, -0.2596692740917206, 0.12303505837917328, 0.09439222514629364, -0.10074996203184128, -0.17879411578178406, 0.4768568277359009, -0.11600562185049057, -0.01781764253973961, -0.0427953340113163, 0.2037140280008316, -0.040384162217378616, -0.09214300662279129, 0.08346299082040787, 0.2805425524711609, -0.16032451391220093, 0.05353733152151108, 0.17788729071617126, -0.11847023665904999, 0.15274454653263092, 0.19623610377311707, -0.5755741000175476, 0.327268123626709, -0.20599013566970825, -0.11946216225624084, 0.12294961512088776, 0.23789742588996887, -0.5433440208435059, -0.008359342813491821, 0.3409183621406555, 0.18958355486392975, -0.054511938244104385, 0.22892341017723083, 0.0896889865398407, -0.28873762488365173, -0.024364445358514786, 0.15803727507591248, -0.29335150122642517, -0.5226261615753174, -0.14600306749343872, -0.09419446438550949, -0.2946050763130188, -0.25676602125167847, -0.1969342827796936, -0.3000229001045227, -0.06669080257415771, 0.1638118177652359, 0.4878987669944763, 0.1474820226430893, 0.30946266651153564, -0.12323392927646637, -0.054084423929452896, 0.18343937397003174, 0.05078648030757904, -0.012930803000926971, -0.43211066722869873, 0.2982901930809021, 0.2857542335987091, -0.15466243028640747, 0.730219841003418, 0.17611876130104065, 0.04827536642551422, 0.15348556637763977, 0.01618134044110775, -0.1423570215702057, -0.07452184706926346, 0.21011528372764587, 0.4342089295387268, -0.11848050355911255, -0.17147710919380188, 0.01606806553900242, 0.10634974390268326, 0.17031270265579224, 0.06255049258470535, -0.16015379130840302, 0.24129648506641388, 0.10273010283708572, -0.3848673105239868, 0.10915562510490417, -0.4559486508369446, -0.061426617205142975, -0.02472063899040222, 0.20129668712615967, 0.1704210638999939, 0.07469676434993744, -0.32020309567451477, 0.14379721879959106, 0.2195436656475067, 0.2836676836013794, 0.008646804839372635, -0.4004831612110138, 0.06336063146591187, 0.25955748558044434, -0.009603627026081085, 0.1099478229880333, -0.23018553853034973, 0.006707429885864258, 0.20969092845916748, 0.3252524733543396, 0.04416371509432793, -0.08415566384792328, 0.05857435241341591, 0.4847150146961212, 0.01538848876953125, -0.078362375497818, -0.29463785886764526, -0.28035029768943787, 0.23457393050193787, 0.33647406101226807, 0.22154881060123444, 0.029484301805496216, -0.2503466308116913, 0.0959387719631195, 0.07571229338645935, -0.5801775455474854, 0.5711284875869751, 0.017507486045360565, -0.08597125113010406, -0.19277828931808472, -0.09723962098360062, 0.2346183955669403, 0.05525911599397659, 0.2244197428226471, -0.09642723947763443, 0.08934183418750763, 0.2038494348526001, 0.26913151144981384, -0.08522709459066391, -0.01506948471069336, 0.5434054136276245, 0.4676169157028198, -0.2189486175775528, 0.03208005055785179, 0.24516907334327698, -0.006806962192058563, -0.17114350199699402, -0.09091731905937195, -0.07825686037540436, 0.10965980589389801, -0.22641822695732117, -0.0797162801027298, -0.1147271916270256, -0.40054744482040405, 0.16050061583518982, -0.019935987889766693, -0.2360002100467682, 0.029080133885145187, -0.39955079555511475, 0.18522930145263672, -0.12844642996788025, 0.11251764744520187, -0.3612789511680603, 0.04101329296827316, 0.08136756718158722, -0.023496003821492195, -0.08920207619667053, 0.06102357804775238, 0.10416008532047272, 0.3786587715148926, -0.45803001523017883, -0.35699230432510376, 0.0026283785700798035, -0.21646764874458313, -0.15505732595920563, 0.09848891943693161, -0.2773807644844055, 0.18726100027561188, 0.05912194028496742, 0.30498120188713074, -0.2787612974643707, -0.1753445863723755, -0.07379000633955002, 0.040038980543613434, -0.06592852622270584, -0.02103276550769806, 0.2447916567325592, 0.3242921233177185, 0.06892316788434982, 0.17421433329582214, 0.05488868057727814, -0.4093170166015625, -0.2168409824371338, 0.1714797019958496, 0.20750698447227478, -0.14964734017848969, 0.3099198043346405, 0.04166501760482788, -0.37110257148742676, -0.1696203649044037, 0.3343539834022522, -0.005060192197561264, -0.01248224824666977, 0.03815920278429985, -0.3507927656173706, 0.025768831372261047, 0.25045275688171387, -0.24948632717132568, -0.29434359073638916, 0.04906037449836731, -0.4468449652194977, 0.3348524272441864, -0.28550922870635986, -0.41664016246795654, 0.4108617901802063, 0.14414307475090027, -0.23864369094371796, 0.019445590674877167, 0.10606812685728073, -0.002392515540122986, -0.10520394891500473, -0.1128360778093338, 0.08665435761213303, 0.14820940792560577, 0.38562434911727905, -0.00397113710641861, -0.018039509654045105, -0.2792937159538269, 0.02062239870429039, 0.25670939683914185, -0.03420333191752434, 0.16862832009792328, -0.28808894753456116, -0.049304112792015076, 0.15615960955619812, 0.14321783185005188, 0.07532443106174469, -0.1431836634874344, -0.29012221097946167, -0.33386892080307007, 0.19578340649604797, -0.23788806796073914, -0.01462707482278347, 0.08322277665138245, 0.09225407987833023, -0.20213013887405396, 0.21762314438819885, -0.11215663701295853, 0.04137016460299492, -0.23029951751232147, -0.5249097943305969, -0.22172322869300842, -0.1592601090669632, 0.22647233307361603, -0.3145913779735565, 0.15120063722133636, 0.07459066063165665, -0.1862061321735382, -0.10754600912332535, -0.36532893776893616, 0.12035602331161499, -0.1996338963508606, -0.1775704324245453, -0.2650453746318817, -0.03514565899968147, 0.07828845083713531, -0.13715295493602753, -0.03973783180117607, -0.009267613291740417, 0.6809391975402832, -0.08328264951705933, -0.27940282225608826, 0.1192643865942955, 0.11072197556495667, -0.10166728496551514, 0.1572708934545517, 0.057215992361307144, 0.4258536696434021, 0.24076785147190094, 0.09451228380203247, -0.11742053180932999, -0.17010502517223358, 0.3406692147254944, -0.032612137496471405, 0.0012197350151836872, 0.2633316218852997, -0.4073575437068939, 0.11138184368610382, 0.5188443660736084, 0.062318798154592514, -0.12208802998065948, -0.12847042083740234, 0.15519127249717712, -0.14657658338546753, -0.3989904224872589, 0.36393681168556213, -0.01389588974416256, 0.3993525207042694, 0.16379250586032867, -0.06417640298604965, -0.11838460713624954, -0.2035062313079834, 0.48245057463645935, 0.3535473048686981, 0.3446825444698334, 0.20214909315109253, -0.18826664984226227, -0.07118625193834305, 0.03592729941010475, 0.1638687551021576, -0.1852387934923172, 0.01347246766090393, -0.14523786306381226, -0.08057558536529541, -0.12267648428678513, -0.14733509719371796, -0.0005671693943440914, 0.2755776643753052, 0.10912556946277618, -0.18889246881008148, 0.0978524312376976, -0.17188580334186554, -0.034711576998233795, -0.05307919904589653, -0.09353774785995483]
single_search_query = "select id, distance('topK=10','ef_s=440')(vector,{}) from testdata_baidu_100w_rc".format(search_vector)


async def async_client_test():
    async with ClientSession() as s:
        async with AsyncClient(s, url="http://10.10.1.51:8123/") as client:
            alive = await client.is_alive()
            print(f"Is ClickHouse alive? -> {alive}")
            res = await client.fetch(query=single_search_query)
            for line in res:
                print(f"{line[0]}---{line[1]}")

async def async_client_test2():
    async with AsyncClient(url="http://10.10.1.51:8123/") as client:
        alive = await client.is_alive()
        print(f"Is ClickHouse alive? -> {alive}")
        async for line in client.iterate(query=single_search_query):
            print(f"{line[0]}---{line[1]}")


def sync_client_test():
    sync_client = Client(url='http://10.10.1.51:8123/', user="default", password="")
    print(f"Is ClickHouse alive? -> {sync_client.is_alive()}")
    for i in sync_client.iterate(query=single_search_query):
        print(f"{i[0]}---{i[1]}")
    print("------")
    row=sync_client.fetchrow(query=single_search_query)
    print(f"{row[0]} {row[1]}")

import asyncio
from myscaledb import AsyncClient
from aiohttp import ClientSession

async def main():
    async with ClientSession() as s:
        async with AsyncClient(s,url="http://10.10.1.100:8123/") as client:
            alive = await client.is_alive()
            print(f"Is Myscale alive? -> {alive}")

if __name__ == '__main__':
    print("\n\nasync test\n\n")
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(async_client_test())
    # asyncio.run(async_client_test())
    # asyncio.run(async_client_test())

    # print("\n\nsync test\n\n")
    # sync_client_test()

    asyncio.run(main())


