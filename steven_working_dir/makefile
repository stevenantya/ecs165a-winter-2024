CPPFLAGS = -std=c++11
SRCS = main.cpp Table.cpp Page.cpp 
OBJS = $(SRCS:.cpp=.o)
DEPS = $(OBJS:.o=.d)

all: run.exe

run.exe: $(OBJS)
	g++ -o $@ $(CPPFLAGS) $^

%.o: %.cpp
	g++ $(CPPFLAGS) -MMD -MP -c $< -o $@

-include $(DEPS)

clean:
	del /F *.o run.exe *.d

.PRECIOUS: $(OBJS)