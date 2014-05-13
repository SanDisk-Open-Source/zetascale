#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include "fdf.h"

int main() 
{
	char fname[1024];
	struct FDF_state* fdf_state;
    FDF_status_t ret = FDFInit(&fdf_state);
	assert(ret);
	sprintf(fname, "/tmp/fdf_listen_pid.%d", getpid());
	int r = open(fname, O_CREAT, 0660);
	assert(r > 0);
	sleep(60);
    (void)FDFShutdown(fdf_state);
}

