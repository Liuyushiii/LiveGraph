#include<iostream>

using namespace std;

uint8_t size_to_order(size_t size)
{
    uint8_t order = (uint8_t)((size & (size - 1)) != 0);
    while (size > 1)
    {
        order += 1;
        size >>= 1;
    }
    return order;
}

int main(){
    cout << "test" << endl;

    size_t order = size_to_order(7);
    cout <<  order << endl;

    order = size_to_order(9);
    cout <<  order << endl;

    return 0;
}