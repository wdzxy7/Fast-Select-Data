import re

if __name__ == '__main__':
    sql = 'SELECT distinct max(score) as big, min(score) as min FROM grades.exponential_sample19;'
    result = re.findall(r'^select(.*?)from', sql, re.IGNORECASE)
    print(result)
    select = result[0]
    selects = select.split(',')
    item = []
    for select in selects:
        select = select.strip()
        select = select.split(' ')
        print(select)
        item.append(select[-1])
    print(item)