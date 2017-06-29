import random
f = open("data.txt","r")
sample = ''.join([line.strip() for line in f.readlines()])
f.close()
origin = "5865e5a7fba95e82a88072bd"
newIds = []
for i in range(10):
	newIds.append("5865e5a7fba95e82a88072b"+str(i))

for i in range(26):
	newIds.append("5865e5a7fba95e82a88072b"+chr(ord('a')+i))

random.shuffle(newIds)
lines = []
for i in newIds:
	lines.append(sample.replace(origin,i))

timezoneIds = []
for i in range(26):
	newIds.append("5865e5a7fba95e82a88072b"+chr(ord('A')+i))
	timezoneIds.append("5865e5a7fba95e82a88072b"+chr(ord('A')+i))
random.shuffle(newIds)
for i in newIds:
	lines.append('{"project_id":"'+i+'","timezone":3000}')
random.shuffle(lines)
for i in timezoneIds:
	lines.append(sample.replace(origin,i))

f = open("data1.txt","w")
f.write('\n'.join(lines))
f.close()
