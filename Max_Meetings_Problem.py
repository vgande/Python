def maxEvents(arrival,duration):
    
    meetingList = []
    result = []
    finish = 0
    
    for i in range(len(arrival)):
        meetingTime = (arrival[i], arrival[i]+duration[i])
        meetingList.append(meetingTime)
        
    meetingList.sort(key=lambda x: x[1])
        
    for meetingHours in meetingList:
        
        if finish <= meetingHours[0]:
            finish = meetingHours[1]
            result.append(meetingHours)
            
    print(result)
    
if "__name__" == "__main__":
    
    count = int(input("Select number of meetings").strip())
    arrival = []
    duration = []

    for _ in range(count):
        arrival_item = int(input("Enter the start time of the meeting").strip())
        arrival.append(arrival_item)

    for _ in range(count):
        duration_item = int(input("Enter duration of the meeting").strip())
        duration.append(duration_item)
    
    maxEvents(arrival,duration)
