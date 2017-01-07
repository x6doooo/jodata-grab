/**
 * Created by dx.yang on 2016/12/24.
 */


function time2show(start, end) {
    let diff = end - start;
    let x = diff / 1000;
    let seconds = ~~(x % 60);
    seconds = seconds < 10 ? '0' + seconds : seconds;
    x /= 60;
    let minutes = ~~(x % 60);
    minutes = minutes < 10 ? '0' + minutes : minutes;
    x /= 60;
    let hours = ~~(x % 24);
    hours = hours < 10 ? '0' + hours : hours;
    return `${hours}:${minutes}:${seconds}`
}


module.exports = {
    time2show
};