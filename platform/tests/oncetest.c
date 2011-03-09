#include "platform/assert.h"
#include "platform/once.h"

static int count = 0;

PLAT_ONCE(static, foo);

PLAT_ONCE_IMPL(static, foo, ++count);

int
main() {
    foo_once();
    plat_assert_always(count == 1);
    foo_once();
    plat_assert_always(count == 1);
    return (0);
}
