OBJS=main.o Mapper.o Pool.o Reducer.o 

build: $(OBJS)
		g++ $(OBJS) -o tema1 -lpthread
clean:
		rm -rf tema1 $(OBJS)

%.o: %.cpp
		g++ $< -c -o $@ -lpthread
