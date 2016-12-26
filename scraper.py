import requests
import getpass
import itertools

if __name__ == "__main__":
    user = raw_input("username")
    pwd = getpass.getpass("password")

    URL = "https://loanperformancedata.fanniemae.com/lppub/loginForm.html"
    payload = {"username":user, "password":pwd, "agreement":"true", "_agreement":"on"}

    session = requests.session()
    session.post(URL,data=payload) #login

    for year, quarter in itertools.product(range(2000,2016), range(1,5)):
        print year, quarter
        fn = "{}Q{}.zip".format(year,quarter)
        data = {"file":fn}
        response = session.get("https://loanperformancedata.fanniemae.com/lppub/publish", params=data, stream=True, allow_redirects=False)
        length = int(response.headers.get('content-length',None))
        if response.ok:
            with open("performance_data/"+fn,"wb") as f:
                total = 0
                CHUNK_SIZE = 1024
                for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                    if not total%(CHUNK_SIZE*512):
                        print total*1./length
                    if chunk:
                        f.write(chunk)
                        total += CHUNK_SIZE
        else:
            print "not ok! {}, {}".format(year, quarter)





