lst = [1,3,2,4,5]
print("lst :",tuple(lst))

#len()function
print("length of lst:",len(tuple(lst)))

#sorted()func
sorted_tuple = sorted(tuple(lst),reverse = True)
print("The sorted tuple is :",sorted_tuple)

#min(),max(),sum() func
print("Min:", min(tuple(lst)))
print("Max:", max(tuple(lst)))
print("Sum:", sum(tuple(lst)))