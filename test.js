/**
 * Created by dx.yang on 2016/12/2.
 */


async function test() {
    return 123
}

(async () => {
    let a = await test()
    console.log(a);
})();
