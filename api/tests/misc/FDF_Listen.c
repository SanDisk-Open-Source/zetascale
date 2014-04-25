#include <unistd.h>
#include <assert.h>
#include "fdf.h"

int main() 
{
	struct FDF_state* fdf_state;
    FDF_status_t ret = FDFInit(&fdf_state);
	assert(ret);
	sleep(60);
    (void)FDFShutdown(fdf_state);
}

