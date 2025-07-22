use std::mem;
use std::mem::MaybeUninit;

pub struct RingBuf<T, const N: usize> {
    head: usize,
    written: usize,
    buf: [MaybeUninit<T>; N],
}

impl<T, const N: usize> RingBuf<T, N> {
    pub fn new() -> Self {
        Self {
            head: 0,
            written: 0,
            buf: [ const { MaybeUninit::uninit() }; N],
        }
    }

    pub fn len(&self) -> usize {
        self.written
    }

    pub fn can_enqueue(&self) -> bool {
        self.written < N
    }

    pub fn enqueue(&mut self, x: T) {
        if self.written == N {
            panic!("Ring buffer is full")
        }

        self.buf[(self.head + self.written) % N] = MaybeUninit::new(x);
        self.written += 1;
    }

    pub fn dequeue(&mut self) -> T {
        if self.written == 0 {
            panic!("Ring buffer is empty")
        }

        let x = mem::replace(&mut self.buf[self.head], MaybeUninit::uninit());
        self.head = (self.head + 1) % N;
        self.written -= 1;
        unsafe { x.assume_init() }
    }
    
    pub fn peek(&self) -> &T {
        if self.written == 0 {
            panic!("Ring buffer is empty")
        }

        let p = self.buf[self.head].as_ptr();
        unsafe { &*p }
    }
}

impl<T, const N: usize> Drop for RingBuf<T, N> {
    fn drop(&mut self) {
        while self.len() != 0 {
            self.dequeue();
        }
    }
}
