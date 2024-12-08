import os
import random

os.system("rm a.txt")
with open("a.txt","w")  as f:
    f.write(str(random.randint(0, 1000)) + "\n")
for i in range(27):
  print(i)
  os.system("cat a.txt > b.txt")
  os.system("cat b.txt >> a.txt")

os.system("rm b.txt")