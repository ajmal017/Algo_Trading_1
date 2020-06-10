starting_row_weight = 62
column_weight = 'AK'

starting_row_data = 2
column_data = 'N'

time = 60 * 2 + 1

formula = '=('

for i in range(time):
   
    formula = formula + '$' + column_weight + '$' + str(starting_row_weight + i) + '*' +\
        column_data + str(starting_row_data + i) + '+'
       
formula = formula[:-1]      # Get rid of final +

formula = formula + ')/' + str(time)

print(formula)
