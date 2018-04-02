import random
import sys
all_states = [ "ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc", "fl",
           "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
           "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
           "ny", "nc", "nd", "oh", "ok", "or", "pa", "pr", "ri", "sc", "sd",
           "tn", "tx", "ut", "vt", "va", "vi", "wa", "wv", "wi", "wy", "al",
           "bc", "mb", "nb", "lb", "nf", "nt", "ns", "nu", "on", "qc", "sk",
           "yt", "dengl", "fraspm" ]
k=sys.argv[1]
seed=sys.argv[2]
random.seed(int(seed))
for i in range(int(k)):
    print(all_states[random.randint(0,len(all_states))])